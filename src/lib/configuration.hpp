#pragma once

#include "utils/literal.hpp"

namespace skyrise {

/**
 * The name of the method that AWS Lambda calls to execute the function.
 */
inline constexpr std::string_view kLambdaFunctionHandler = "FunctionHandler";

/**
 * The name of the AWS IAM role for the Lambda function.
 */
inline constexpr std::string_view kLambdaFunctionRoleName = "AWSLambda";

/**
 * After creation, an AWS Lambda function is initially in a pending state until all required resources are available
 * (cf. https://docs.aws.amazon.com/lambda/latest/dg/functions-states.html). We wait and poll for the function's state
 * to become active before we start invoking the function. Based on our measurements, this process is not supposed to
 * take longer than 10 seconds.
 */
inline constexpr size_t kLambdaFunctionStatePollingTimeoutSeconds = 10;

/**
 * The interval length for polling a function's state.
 */
inline constexpr size_t kLambdaFunctionStatePollingIntervalMilliseconds = 100;

/**
 * The AWS Lambda service-side timeout limit for functions is 15 minutes (cf.
 * https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html). Our own timeout is reduced to 5 minutes,
 * because we expect our cloud functions to never run longer than that. Thereby, we keep costs for erroneous function
 * invocations that never terminate in check.
 */
inline constexpr size_t kLambdaFunctionTimeoutSeconds = 300;

/**
 * The maximum size for files to be read from the local filesystem.
 */
inline constexpr size_t kMaxFileSize = 2_GB;

}  // namespace skyrise
