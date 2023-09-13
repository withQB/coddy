package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

const usage = `Usage: %s

Generate key files which are required by dendrite.

Arguments:

`

var (
	tlsCertFile       = flag.String("tls-cert", "", "An X509 certificate file to generate for use for TLS")
	tlsKeyFile        = flag.String("tls-key", "", "An RSA private key file to generate for use for TLS")
	privateKeyFile    = flag.String("private-key", "", "An Ed25519 private key to generate for use for object signing")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, usage, os.Args[0])
		flag.PrintDefaults()
	}

	flag.Parse()

	if *tlsCertFile == "" && *tlsKeyFile == "" && *privateKeyFile == "" {
		flag.Usage()
		return
	}

	if *tlsCertFile != "" || *tlsKeyFile != "" {
		if *tlsCertFile == "" || *tlsKeyFile == "" {
			log.Fatal("Zero or both of --tls-key and --tls-cert must be supplied")
		}
		
		fmt.Printf("Created TLS cert file:    %s\n", *tlsCertFile)
		fmt.Printf("Created TLS key file:     %s\n", *tlsKeyFile)
	}

	if *privateKeyFile != "" {
		fmt.Printf("Created private key file: %s\n", *privateKeyFile)
	}
}
