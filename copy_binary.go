package opengauss

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// copyBinarySignature starts every COPY ... TO STDOUT binary stream.
var copyBinarySignature = []byte("PGCOPY\n\xff\r\n\x00")

// copyBinaryHeaderLen is the signature, the flags and the extension length.
const copyBinaryHeaderLen = 11 + 4 + 4

// consumeCopyBinaryHeader reports the size of the stream header, whose flags
// must be zero and whose extension area is skipped. ok is false when the buffer
// does not hold the whole header yet.
func consumeCopyBinaryHeader(buf []byte) (consumed int, ok bool, err error) {
	if len(buf) < copyBinaryHeaderLen {
		return 0, false, nil
	}
	if !bytes.Equal(buf[:len(copyBinarySignature)], copyBinarySignature) {
		return 0, false, errors.New("opengauss: the COPY ... TO STDOUT binary stream has an unknown signature")
	}
	flags := int32(binary.BigEndian.Uint32(buf[len(copyBinarySignature):]))
	if flags != 0 {
		return 0, false, fmt.Errorf("opengauss: the COPY ... TO STDOUT binary stream sets unsupported flags %#x", uint32(flags))
	}
	extension := int32(binary.BigEndian.Uint32(buf[len(copyBinarySignature)+4:]))
	if extension < 0 {
		return 0, false, errors.New("opengauss: the COPY ... TO STDOUT binary stream has a negative header length")
	}
	total := copyBinaryHeaderLen + int(extension)
	if len(buf) < total {
		return 0, false, nil
	}
	return total, true, nil
}

// parseCopyBinaryTuple reads one tuple: a field count, then every field as a
// length-prefixed value with -1 for NULL. A field count of -1 is the trailer,
// which is reported through ended. ok is false when the buffer does not hold a
// whole tuple yet; the length prefixes are walked first, so a partial tuple
// allocates nothing before the next frame arrives.
func parseCopyBinaryTuple(buf []byte, columns int) (fields []copyField, consumed int, ended bool, ok bool, err error) {
	if len(buf) < 2 {
		return nil, 0, false, false, nil
	}
	count := int16(binary.BigEndian.Uint16(buf[:2]))
	if count == -1 {
		return nil, 2, true, true, nil
	}
	if count < 0 {
		return nil, 0, false, false, fmt.Errorf("opengauss: the COPY ... TO STDOUT binary stream has an invalid field count %d", count)
	}
	if int(count) != columns {
		return nil, 0, false, false, fmt.Errorf("opengauss: a COPY ... TO STDOUT binary row has %d fields but the stream announced %d columns", count, columns)
	}
	i := 2
	for field := 0; field < int(count); field++ {
		if len(buf)-i < 4 {
			return nil, 0, false, false, nil
		}
		length := int32(binary.BigEndian.Uint32(buf[i:]))
		i += 4
		if length == -1 {
			continue
		}
		if length < 0 {
			return nil, 0, false, false, fmt.Errorf("opengauss: the COPY ... TO STDOUT binary stream has an invalid field length %d", length)
		}
		if int64(length) > int64(len(buf)-i) {
			return nil, 0, false, false, nil
		}
		i += int(length)
	}
	// The whole tuple is present, so the fields can point into the buffer.
	fields = make([]copyField, int(count))
	i = 2
	for field := 0; field < int(count); field++ {
		length := int32(binary.BigEndian.Uint32(buf[i:]))
		i += 4
		if length == -1 {
			fields[field] = copyField{null: true}
			continue
		}
		fields[field] = copyField{value: buf[i : i+int(length)]}
		i += int(length)
	}
	return fields, i, false, true, nil
}
