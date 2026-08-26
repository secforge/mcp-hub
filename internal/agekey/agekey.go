// Package agekey validates the *format* of an age
// (https://age-encryption.org) X25519 recipient (public key) string. It
// does not parse, decode, or use the key for any cryptographic purpose —
// the hub never encrypts or decrypts anything; it only checks that a
// peer-supplied string is structurally a well-formed age public key
// (correct bech32 "age1..." encoding with a valid checksum) before
// distributing it to other peers, who may use it however they choose.
package agekey

import "strings"

// bech32Charset is the alphabet used by bech32 encoding (BIP-173) — the
// encoding age public keys use. It deliberately excludes '1', 'b', 'i', 'o'
// to avoid visual ambiguity.
const bech32Charset = "qpzry9x8gf2tvdw0s3jn54khce6mua7l"

const hrp = "age"

// Valid reports whether s is a well-formed age public key: an all-lowercase
// bech32 string with human-readable part "age" (i.e. starting "age1") and a
// valid bech32 checksum. This is a pure format check, per BIP-173 — it says
// nothing about whether the key actually corresponds to any real recipient.
func Valid(s string) bool {
	if len(s) < len(hrp)+1+6 || len(s) > 90 {
		return false
	}
	if s != strings.ToLower(s) {
		return false
	}
	if !strings.HasPrefix(s, hrp+"1") {
		return false
	}
	dataPart := s[len(hrp)+1:]
	data := make([]int, len(dataPart))
	for i, c := range dataPart {
		idx := strings.IndexRune(bech32Charset, c)
		if idx < 0 {
			return false
		}
		data[i] = idx
	}
	return verifyChecksum(hrp, data)
}

func verifyChecksum(hrp string, data []int) bool {
	return polymod(append(hrpExpand(hrp), data...)) == 1
}

func hrpExpand(hrp string) []int {
	ret := make([]int, 0, len(hrp)*2+1)
	for _, c := range hrp {
		ret = append(ret, int(c)>>5)
	}
	ret = append(ret, 0)
	for _, c := range hrp {
		ret = append(ret, int(c)&31)
	}
	return ret
}

func polymod(values []int) int {
	generator := [5]int{0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3}
	chk := 1
	for _, v := range values {
		top := chk >> 25
		chk = (chk&0x1ffffff)<<5 ^ v
		for i := 0; i < 5; i++ {
			if (top>>uint(i))&1 == 1 {
				chk ^= generator[i]
			}
		}
	}
	return chk
}
