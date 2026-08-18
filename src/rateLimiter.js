class TokenBucket {
  constructor(ratePerMinute, burst, now = Date.now()) {
    this.capacity = burst;
    this.tokens = burst;
    this.tokensPerMs = ratePerMinute / 60000;
    this.lastRefillAt = now;
  }

  consume(now = Date.now()) {
    const elapsed = Math.max(0, now - this.lastRefillAt);
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.tokensPerMs);
    this.lastRefillAt = now;
    if (this.tokens >= 1) {
      this.tokens -= 1;
      return { allowed: true, retryAfter: 0 };
    }
    const waitMs = (1 - this.tokens) / this.tokensPerMs;
    return { allowed: false, retryAfter: Math.max(1, Math.ceil(waitMs / 1000)) };
  }
}

module.exports = { TokenBucket };
