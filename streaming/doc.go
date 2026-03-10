// Package streaming provides a client for consuming ATProto event streams
// (the "firehose"). It handles WebSocket connection management, automatic
// reconnection with exponential backoff, cursor tracking, and decoding of
// repository commit events into individual record operations.
package streaming
