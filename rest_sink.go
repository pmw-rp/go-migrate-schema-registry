package main

import (
	"context"
	"crypto/tls"
	"errors"
	"github.com/twmb/franz-go/pkg/sr"
	"net/http"
)

type RestSink struct {
	URL      string      `koanf:"url"`
	Username string      `koanf:"username"`
	Password string      `koanf:"password"`
	TLS      *tls.Config `koanf:"tls"`

	Ctx    context.Context
	client *sr.Client
}

func (r *RestSink) Connect() error {
	opts := make([]sr.ClientOpt, 0)
	opts = append(opts, sr.URLs(r.URL))
	opts = append(opts, sr.BasicAuth(r.Username, r.Password))
	opts = append(opts, sr.DialTLSConfig(r.TLS))
	client, err := sr.NewClient(opts...)
	if err != nil {
		return err
	}
	r.Ctx = sr.WithParams(context.Background(), sr.ShowDeleted)
	r.client = client
	return nil
}

func (r *RestSink) PutState(state *State) error {

	// Set the global compatibility mode to NONE during import process
	globalCompat := r.client.Compatibility(r.Ctx)[0]
	none := toSetCompatibilityResult(globalCompat)
	none.Level = sr.CompatNone
	r.client.SetCompatibility(r.Ctx, none)
	// Reset the global compatibility mode to its original setting once we're done
	defer r.client.SetCompatibility(r.Ctx, toSetCompatibilityResult(globalCompat))

	// Cache compatibility results as we go
	localCompatibilities := make(map[string]sr.CompatibilityResult)

	for _, ss := range state.SubjectSchemas {

		pre, err := r.client.LookupSchema(r.Ctx, ss.Subject, ss.Schema)

		// if it doesn't exist, create it
		// if it does exist, check the version / subject / ID matches what we already have
		// if they don't match, give up - refuse to process further

		if err != nil {
			var responseError *sr.ResponseError
			if errors.As(err, &responseError) {
				if responseError.StatusCode == http.StatusNotFound {

					// check if we've previously set this subject's compat to none (no need to do it twice)
					_, ok := localCompatibilities[ss.Subject]
					if !ok {
						// We haven't, so check with SR to see if there is a subject-level compatibility set
						subjectCompat := r.client.Compatibility(r.Ctx, ss.Subject)[0]
						if subjectCompat.Level != 0 {
							// There is, so we set it to none, and put store the original subject-level compat in
							// a local map for later.
							r.client.SetCompatibility(r.Ctx, sr.SetCompatibility{
								Level: sr.CompatNone,
							}, ss.Subject)
							localCompatibilities[ss.Subject] = subjectCompat
						}
					}

					// Looks like the schema doesn't exist, so we create it
					result, err := r.client.CreateSchemaWithIDAndVersion(r.Ctx, ss.Subject, ss.Schema, ss.ID, ss.Version)
					if err != nil {
						panic(err)
					} else {
						// We created a new schema, but does it have the right ID?
						if result.ID != ss.ID {
							err = r.client.DeleteSchema(r.Ctx, result.Subject, result.Version, sr.HardDelete)
							if err != nil {
								// Created a version with the wrong ID, oops
								panic(err)
							}
						}
					}

				} else {
					// some other error occurred
					panic(err)
				}
			} else {
				// some non-responseerror came back, which should be impossible
				panic("got an error that wasn't an sr.ResponseError, which should be impossible")
			}
		} else {
			// The schema exists
			if !(pre.Subject == ss.Subject && pre.Version == ss.Version && pre.ID == ss.ID) {
				panic("the schema already exists but with a different subject / version / id")
			}
		}

	}

	// Iterate over the subject compatibility results we accumulated locally
	for subject, compatibilityResult := range localCompatibilities {
		r.client.SetCompatibility(r.Ctx, toSetCompatibilityResult(compatibilityResult), subject)
	}

	//Write subject compatibilities that were in the state
	for _, result := range state.CompatibilityResults {
		r.client.SetCompatibility(r.Ctx, toSetCompatibilityResult(result), result.Subject)
	}

	// Process Soft Deletions
	for _, subjectVersion := range state.SoftDeletions {
		err := r.client.DeleteSchema(r.Ctx, subjectVersion.Subject, subjectVersion.Version, sr.SoftDelete)
		if err != nil {
			panic(err)
		}
	}
	return nil
}

func toSetCompatibilityResult(compatibilityResult sr.CompatibilityResult) sr.SetCompatibility {
	return sr.SetCompatibility{
		Level:            compatibilityResult.Level,
		Alias:            compatibilityResult.Alias,
		Normalize:        compatibilityResult.Normalize,
		Group:            compatibilityResult.Group,
		DefaultMetadata:  compatibilityResult.DefaultMetadata,
		OverrideMetadata: compatibilityResult.OverrideMetadata,
		DefaultRuleSet:   compatibilityResult.DefaultRuleSet,
		OverrideRuleSet:  compatibilityResult.OverrideRuleSet,
	}
}
