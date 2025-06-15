#!/usr/bin/env python3

import json
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pathlib import Path
import argparse

def load_benchmark_results(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def plot_latency_distribution(data, operation, output_dir):
    latencies = data[operation]['latencies_ms']
    
    plt.figure(figsize=(10, 6))
    sns.histplot(latencies, kde=True)
    plt.title(f'{operation.capitalize()} Latency Distribution')
    plt.xlabel('Latency (ms)')
    plt.ylabel('Count')
    plt.grid(True, alpha=0.3)
    
    # Add statistics as text
    stats = f"Min: {data[operation]['min_latency_ms']:.2f} ms\n"
    stats += f"Max: {data[operation]['max_latency_ms']:.2f} ms\n"
    stats += f"Avg: {data[operation]['avg_latency_ms']:.2f} ms"
    
    plt.text(0.95, 0.95, stats,
             transform=plt.gca().transAxes,
             verticalalignment='top',
             horizontalalignment='right',
             bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.savefig(output_dir / f'{operation}_latency_distribution.png')
    plt.close()

def plot_throughput_comparison(data, output_dir):
    operations = ['write', 'read']
    throughputs = [data[op]['throughput_mbps'] for op in operations]
    
    plt.figure(figsize=(8, 6))
    bars = plt.bar(operations, throughputs)
    
    # Add value labels on top of bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.2f} MB/s',
                ha='center', va='bottom')
    
    plt.title('Throughput Comparison')
    plt.ylabel('Throughput (MB/s)')
    plt.grid(True, alpha=0.3)
    
    plt.savefig(output_dir / 'throughput_comparison.png')
    plt.close()

def plot_latency_boxplot(data, output_dir):
    operations = ['write', 'read']
    latencies = [data[op]['latencies_ms'] for op in operations]
    
    plt.figure(figsize=(8, 6))
    plt.boxplot(latencies, labels=operations)
    plt.title('Latency Distribution Comparison')
    plt.ylabel('Latency (ms)')
    plt.grid(True, alpha=0.3)
    
    plt.savefig(output_dir / 'latency_boxplot.png')
    plt.close()

def create_summary_report(data, output_dir):
    params = data['parameters']
    report = f"""Benchmark Summary Report
=====================

Parameters:
-----------
Object Size: {params['object_size_kb']} KB
Object Count: {params['object_count']}
Thread Count: {params['thread_count']}
Bucket: {params['bucket_name']}

Write Results:
-------------
Min Latency: {data['write']['min_latency_ms']:.2f} ms
Max Latency: {data['write']['max_latency_ms']:.2f} ms
Avg Latency: {data['write']['avg_latency_ms']:.2f} ms
Throughput: {data['write']['throughput_mbps']:.2f} MB/s
Total Duration: {data['write']['duration_ms']:.2f} ms

Read Results:
------------
Min Latency: {data['read']['min_latency_ms']:.2f} ms
Max Latency: {data['read']['max_latency_ms']:.2f} ms
Avg Latency: {data['read']['avg_latency_ms']:.2f} ms
Throughput: {data['read']['throughput_mbps']:.2f} MB/s
Total Duration: {data['read']['duration_ms']:.2f} ms
"""
    
    with open(output_dir / 'summary_report.txt', 'w') as f:
        f.write(report)

def main():
    parser = argparse.ArgumentParser(description='Visualize storage benchmark results')
    parser.add_argument('results_file', help='JSON file containing benchmark results')
    parser.add_argument('--output-dir', default='benchmark_plots',
                       help='Directory to save visualization outputs')
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    # Load and process data
    data = load_benchmark_results(args.results_file)
    
    # Generate visualizations
    plot_latency_distribution(data, 'write', output_dir)
    plot_latency_distribution(data, 'read', output_dir)
    plot_throughput_comparison(data, output_dir)
    plot_latency_boxplot(data, output_dir)
    create_summary_report(data, output_dir)
    
    print(f"Visualizations saved to {output_dir}")

if __name__ == '__main__':
    main() 