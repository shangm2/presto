// In TaskResource.h
template<typename Func>
auto executeWithStreamIsolation(
    Func&& func,
    proxygen::ResponseHandler* downstream,
    std::shared_ptr<http::CallbackRequestHandlerState> handlerState) {
  try {
    return std::forward<Func>(func)();
  } catch (const nlohmann::json::exception& e) {
    // Specifically catch JSON parsing errors
    LOG(ERROR) << "JSON parsing error in stream (HTTP/2 safe): " << e.what();
    if (!handlerState->requestExpired()) {
      http::sendErrorResponse(downstream, 
          std::string("JSON deserialization error: ") + e.what());
    }
    throw; // Re-throw for thenError to handle
  } catch (const std::exception& e) {
    LOG(ERROR) << "Exception in HTTP/2 stream (isolated): " << e.what();
    if (!handlerState->requestExpired()) {
      http::sendErrorResponse(downstream, 
          std::string("Stream error: ") + e.what());
    }
    throw;
  }
}

// Usage:
folly::via(httpSrvCpuExecutor_,
    [this, &body, taskId, ...]() {
      return executeWithStreamIsolation(
          [&]() {
            // All risky deserialization code here
            std::string requestBody = util::extractMessageBody(body);
            return createOrUpdateFunc(...);
          },
          downstream,
          handlerState);
    })
