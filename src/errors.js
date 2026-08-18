class AppError extends Error {
  constructor(statusCode, code, message, options = {}) {
    super(message);
    this.name = "AppError";
    this.statusCode = statusCode;
    this.code = code;
    this.retryAfter = options.retryAfter;
  }
}

class RconError extends Error {
  constructor(code, message, options = {}) {
    super(message);
    this.name = "RconError";
    this.code = code;
    this.safeToRetry = Boolean(options.safeToRetry);
    this.deliveryUnknown = Boolean(options.deliveryUnknown);
  }
}

module.exports = { AppError, RconError };
