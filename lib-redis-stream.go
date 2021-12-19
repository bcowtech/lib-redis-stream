package redis

func FromStreamOffset(stream, offset string) StreamOffset {
	return StreamOffset{
		Stream: stream,
		Offset: offset,
	}
}

func FromStreamZeroOffset(stream string) StreamOffset {
	return StreamOffset{
		Stream: stream,
		Offset: StreamZeroOffset,
	}
}

func FromStreamNeverDeliveredOffset(stream string) StreamOffset {
	return StreamOffset{
		Stream: stream,
		Offset: StreamNeverDeliveredOffset,
	}
}
