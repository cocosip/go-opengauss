package auth

import (
	"encoding/binary"
	"math/bits"
)

// sm3Sum returns the SM3 hash of data (GB/T 32905-2016). The driver needs it
// only for the stored-key step of the openGauss SM3 authentication exchange,
// and implementing it here keeps the module free of cryptographic
// dependencies.
func sm3Sum(data []byte) []byte {
	state := [8]uint32{
		0x7380166f, 0x4914b2b9, 0x172442d7, 0xda8a0600,
		0xa96f30bc, 0x163138aa, 0xe38dee4d, 0xb0fb0e4e,
	}

	full := len(data) / 64 * 64
	for i := 0; i < full; i += 64 {
		sm3Block(&state, data[i:i+64])
	}

	var tail [128]byte
	padded := copy(tail[:], data[full:])
	tail[padded] = 0x80
	bitLen := uint64(len(data)) * 8
	if padded >= 56 {
		binary.BigEndian.PutUint64(tail[120:], bitLen)
		sm3Block(&state, tail[0:64])
		sm3Block(&state, tail[64:128])
	} else {
		binary.BigEndian.PutUint64(tail[56:], bitLen)
		sm3Block(&state, tail[0:64])
	}

	digest := make([]byte, 32)
	for i, word := range state {
		binary.BigEndian.PutUint32(digest[i*4:], word)
	}
	return digest
}

func sm3Block(state *[8]uint32, block []byte) {
	var w [68]uint32
	var w1 [64]uint32
	for i := 0; i < 16; i++ {
		w[i] = binary.BigEndian.Uint32(block[i*4:])
	}
	for i := 16; i < 68; i++ {
		w[i] = sm3P1(w[i-16]^w[i-9]^bits.RotateLeft32(w[i-3], 15)) ^
			bits.RotateLeft32(w[i-13], 7) ^ w[i-6]
	}
	for i := 0; i < 64; i++ {
		w1[i] = w[i] ^ w[i+4]
	}

	a, b, c, d, e, f, g, h := state[0], state[1], state[2], state[3],
		state[4], state[5], state[6], state[7]
	for i := 0; i < 64; i++ {
		a12 := bits.RotateLeft32(a, 12)
		ss1 := bits.RotateLeft32(a12+e+bits.RotateLeft32(sm3T[i], i%32), 7)
		ss2 := ss1 ^ a12
		tt1 := sm3FF(i, a, b, c) + d + ss2 + w1[i]
		tt2 := sm3GG(i, e, f, g) + h + ss1 + w[i]
		d = c
		c = bits.RotateLeft32(b, 9)
		b = a
		a = tt1
		h = g
		g = bits.RotateLeft32(f, 19)
		f = e
		e = sm3P0(tt2)
	}

	state[0] ^= a
	state[1] ^= b
	state[2] ^= c
	state[3] ^= d
	state[4] ^= e
	state[5] ^= f
	state[6] ^= g
	state[7] ^= h
}

var sm3T = [64]uint32{
	0x79cc4519, 0x79cc4519, 0x79cc4519, 0x79cc4519,
	0x79cc4519, 0x79cc4519, 0x79cc4519, 0x79cc4519,
	0x79cc4519, 0x79cc4519, 0x79cc4519, 0x79cc4519,
	0x79cc4519, 0x79cc4519, 0x79cc4519, 0x79cc4519,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
	0x7a879d8a, 0x7a879d8a, 0x7a879d8a, 0x7a879d8a,
}

func sm3FF(j int, x, y, z uint32) uint32 {
	if j < 16 {
		return x ^ y ^ z
	}
	return (x & y) | (x & z) | (y & z)
}

func sm3GG(j int, x, y, z uint32) uint32 {
	if j < 16 {
		return x ^ y ^ z
	}
	return (x & y) | (^x & z)
}

func sm3P0(x uint32) uint32 {
	return x ^ bits.RotateLeft32(x, 9) ^ bits.RotateLeft32(x, 17)
}

func sm3P1(x uint32) uint32 {
	return x ^ bits.RotateLeft32(x, 15) ^ bits.RotateLeft32(x, 23)
}
