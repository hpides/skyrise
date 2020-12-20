#include "writer.hpp"

namespace skyrise {

TableWriter::TableWriter(TableWriterConfig config, std::shared_ptr<Storage> storage)
    : config_(std::move(config)), queue_(config.queue_capacity), storage_(std::move(storage)) {
  StartWorkers(config_.num_threads);
}

void TableWriter::StartWorkers(size_t n) {
  for (size_t i = 0; i < n; i++) {
    threads_.emplace_back(&TableWriter::ProcessChunkLoop, this);
  }
}

TableWriter::~TableWriter() { Finalize(); }

void TableWriter::ReportError(const StorageError& error) {
  if (has_error_ || !error) {
    return;
  }

  std::lock_guard<std::mutex> guard(error_mutex_);
  if (!error_) {
    error_ = error;
    has_error_ = true;
  }
}

StorageError TableWriter::GetError() {
  std::lock_guard<std::mutex> guard(error_mutex_);
  return error_;
}

void TableWriter::WriteChunk(std::shared_ptr<Chunk> chunk) {
  // Because `nullptr` will cause the worker to exit, we don't want to put `nullptr`s in the queue here.
  if (chunk) {
    queue_.Push(std::move(chunk));
  }
}

void TableWriter::Finalize() {
  for (size_t i = 0; i < threads_.size(); i++) {
    // `nullptr` will signal the worker to stop.
    queue_.Push(nullptr);
  }

  for (auto& thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }

  queue_.Close();
  threads_.clear();
}

void TableWriter::ProcessChunkLoop() {
  size_t num_rows_written = 0;
  std::unique_ptr<skyrise::AbstractFormatter> formatter;
  std::unique_ptr<ObjectWriter> output_object;
  StorageError error = StorageError::Success();

  auto writer_callback = [&output_object, &error](const char* data, size_t length) {
    error = output_object->Write(data, length);
  };
  auto flush = [&]() {
    formatter->Finalize();
    if (error) {
      ReportError(error);
    }
    error = output_object->Close();
    if (error) {
      ReportError(error);
    }
    formatter.reset(nullptr);
    output_object.reset(nullptr);
  };

  std::shared_ptr<Chunk> chunk;
  while (true) {
    queue_.Pop(&chunk);

    // A `nullptr` in the queue is the *only* way how a worker can be stopped.
    if (!chunk) {
      break;
    }

    // If any worker reported an error, we stay in the loop to flush the queue. This way, we make sure
    // that a synchronized exit will happen.
    if (has_error_) {
      continue;
    }

    if (!formatter) {
      // Iff `formatter` is a `nullptr`, `output_object` is a `nullptr` too.
      output_object = storage_->OpenForWriting(config_.naming_strategy(object_id_counter_++));
      formatter = config_.format_factory->Get();
      formatter->SetOutput(writer_callback);
      formatter->Initialize(config_.schema);
    }

    formatter->ProcessChunk(*chunk);
    if (error) {
      ReportError(error);
      continue;
    }

    num_rows_written += chunk->Size();

    if (config_.split_rows != 0 && num_rows_written >= config_.split_rows) {
      flush();
      num_rows_written = 0;
    }
  }

  if (formatter) {
    flush();
  }
}

}  // namespace skyrise
