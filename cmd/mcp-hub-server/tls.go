package main

func useTLS(certFile, keyFile string) bool {
	return certFile != "" && keyFile != ""
}
