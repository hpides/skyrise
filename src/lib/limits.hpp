#pragma once

namespace skyrise {

/**
 * The AWS Lambda service-side timeout limit for functions is 15 minutes (cf.
 * https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html). Our own timeout is reduced to 5 minutes,
 * because we expect our cloud functions to never run longer than that. Thereby, we keep costs for erroneous function
 * invocations that never terminate in check.
 */
const size_t kLambdaFunctionTimeoutSeconds = 300;

}  // namespace skyrise
