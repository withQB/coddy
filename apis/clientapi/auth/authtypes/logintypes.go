package authtypes

// LoginType are specified by login-types
type LoginType string

// The relevant login types implemented in Dendrite
const (
	LoginTypePassword           = "m.login.password"
	LoginTypeDummy              = "m.login.dummy"
	LoginTypeSharedSecret       = "org.coddy.login.shared_secret"
	LoginTypeRecaptcha          = "m.login.recaptcha"
	LoginTypeApplicationService = "m.login.application_service"
	LoginTypeToken              = "m.login.token"
)
