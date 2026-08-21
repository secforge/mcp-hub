package main

import "testing"

func TestUseTLSRequiresBothCertAndKey(t *testing.T) {
	cases := []struct {
		cert, key string
		want      bool
	}{
		{"", "", false},
		{"cert.pem", "", false},
		{"", "key.pem", false},
		{"cert.pem", "key.pem", true},
	}
	for _, c := range cases {
		if got := useTLS(c.cert, c.key); got != c.want {
			t.Errorf("useTLS(%q, %q) = %v, want %v", c.cert, c.key, got, c.want)
		}
	}
}
