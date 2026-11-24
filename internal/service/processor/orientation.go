package processor

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
)

// Only IFD0's scalar orientation is needed; do not traverse untrusted TIFF directories.
func tiffOrientation(data []byte) (int, error) {
	if bytes.HasPrefix(data, []byte("Exif\x00\x00")) {
		data = data[6:]
	}
	invalid := errors.New("invalid EXIF orientation metadata")
	if len(data) < 8 {
		return 0, invalid
	}
	var order binary.ByteOrder
	switch string(data[:2]) {
	case "II":
		order = binary.LittleEndian
	case "MM":
		order = binary.BigEndian
	default:
		return 0, invalid
	}
	if order.Uint16(data[2:4]) != 42 {
		return 0, invalid
	}
	offset := uint64(order.Uint32(data[4:8]))
	if offset < 8 || offset+2 > uint64(len(data)) {
		return 0, invalid
	}
	count := uint64(order.Uint16(data[offset : offset+2]))
	start := offset + 2
	if count > (uint64(len(data))-start)/12 {
		return 0, invalid
	}
	for i := uint64(0); i < count; i++ {
		tag := data[start+i*12 : start+(i+1)*12]
		if order.Uint16(tag[:2]) != 0x112 {
			continue
		}
		if order.Uint16(tag[2:4]) != 3 || order.Uint32(tag[4:8]) != 1 {
			return 0, invalid
		}
		value := int(order.Uint16(tag[8:10]))
		if value < 1 || value > 8 {
			return 0, invalid
		}
		return value, nil
	}
	return 1, nil
}

func readOrientation(ctx context.Context, data []byte, format string) (int, error) {
	if format == "png" {
		for offset := 8; offset+12 <= len(data); {
			if e := ctx.Err(); e != nil {
				return 0, e
			}
			n := uint64(binary.BigEndian.Uint32(data[offset : offset+4]))
			end := uint64(offset) + 12 + n
			if end > uint64(len(data)) {
				return 0, errors.New("truncated PNG chunk")
			}
			if string(data[offset+4:offset+8]) == "eXIf" {
				return tiffOrientation(data[offset+8 : uint64(offset)+8+n])
			}
			offset = int(end)
		}
		return 1, nil
	}
	for offset := 2; offset < len(data); {
		if e := ctx.Err(); e != nil {
			return 0, e
		}
		if data[offset] != 0xff {
			return 0, errors.New("invalid JPEG marker")
		}
		for offset < len(data) && data[offset] == 0xff {
			offset++
		}
		if offset >= len(data) {
			break
		}
		marker := data[offset]
		offset++
		if marker == 0xda || marker == 0xd9 {
			return 1, nil
		}
		if marker == 0x01 || (marker >= 0xd0 && marker <= 0xd7) {
			continue
		}
		if offset+2 > len(data) {
			break
		}
		n := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		if n < 2 || offset+n > len(data) {
			break
		}
		segment := data[offset+2 : offset+n]
		if marker == 0xe1 && bytes.HasPrefix(segment, []byte("Exif\x00\x00")) {
			return tiffOrientation(segment)
		}
		offset += n
	}
	return 0, errors.New("truncated JPEG metadata")
}
