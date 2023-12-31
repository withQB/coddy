package setup

import (
	"flag"
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/withqb/coddy/internal"
	"github.com/withqb/coddy/setup/config"
)

var (
	configPath                            = flag.String("config", "dendrite.yaml", "The path to the config file. For more information, see the config file in this repository.")
	version                               = flag.Bool("version", false, "Shows the current version and exits immediately.")
	enableRegistrationWithoutVerification = flag.Bool("really-enable-open-registration", false, "This allows open registration without secondary verification (reCAPTCHA). This is NOT RECOMMENDED and will SIGNIFICANTLY increase the risk that your server will be used to send spam or conduct attacks, which may result in your server being banned from frames.")
)

// ParseFlags parses the commandline flags and uses them to create a config.
func ParseFlags(monolith bool) *config.Dendrite {
	flag.Parse()

	if *version {
		fmt.Println(internal.VersionString())
		os.Exit(0)
	}

	if *configPath == "" {
		logrus.Fatal("--config must be supplied")
	}

	cfg, err := config.Load(*configPath)

	if err != nil {
		logrus.Fatalf("Invalid config file: %s", err)
	}

	if *enableRegistrationWithoutVerification {
		cfg.ClientAPI.OpenRegistrationWithoutVerificationEnabled = true
	}

	return cfg
}
