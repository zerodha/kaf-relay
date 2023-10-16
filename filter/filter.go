// Package filter provides interfaces and structures for creating and managing filters.
package filter

// Provider is an interface for filter providers. It defines methods for getting the ID of a provider and
// checking if a message is allowed by the provider.
type Provider interface {
	// ID returns the unique identifier of the filter provider.
	ID() string

	// IsAllowed checks if the provided message is allowed by the filter. It returns true if the message is allowed,
	// false otherwise.
	IsAllowed(msg []byte) bool
}

// Config is a structure for holding the configuration of a filter provider. The configuration is represented as a string.
type Config struct {
	Config string `koanf:"config"`
}
