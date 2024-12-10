package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/sr"
	"maps"
	"slices"
)

type RestSource struct {
	URL      string      `koanf:"url"`
	Username string      `koanf:"username"`
	Password string      `koanf:"password"`
	TLS      *tls.Config `koanf:"tls"`

	Ctx    context.Context
	client *sr.Client
}

func (r *RestSource) Connect() {
	opts := make([]sr.ClientOpt, 0)
	opts = append(opts, sr.URLs(r.URL))
	opts = append(opts, sr.BasicAuth(r.Username, r.Password))
	opts = append(opts, sr.DialTLSConfig(r.TLS))
	client, err := sr.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	r.Ctx = sr.WithParams(context.Background(), sr.ShowDeleted)
	r.client = client
}

func filter[T any](ss []T, test func(T) bool) (ret []T) {
	for _, s := range ss {
		if test(s) {
			ret = append(ret, s)
		}
	}
	return
}

func toMap[T comparable](ss []T) map[T]bool {
	result := make(map[T]bool)
	for _, s := range ss {
		result[s] = true
	}
	return result
}

func (r *RestSource) getSubjects() (map[string]bool, map[string]bool, error) {
	deletedSubjectsResponse, err := r.client.Subjects(r.Ctx) // 482
	if err != nil {
		return nil, nil, fmt.Errorf("unable to retrieve subjects (incl deleted): %w", err)
	}
	deletedSubjects := toMap(deletedSubjectsResponse)

	subjectsResponse, err := r.client.Subjects(r.Ctx) // 482
	if err != nil && len(deletedSubjects) == 0 {
		return nil, nil, fmt.Errorf("unable to retrieve subjects: %w", err)
	}
	subjects := toMap(subjectsResponse)

	for _, subject := range subjectsResponse {
		delete(deletedSubjects, subject)
	}

	return subjects, deletedSubjects, nil
}

func (r *RestSource) getVersions(subject string) (map[int]bool, map[int]bool, error) {
	deletedVersionsResponse, err := r.client.SubjectVersions(r.Ctx, subject) // 482
	if err != nil {
		return nil, nil, fmt.Errorf("unable to retrieve subject versions (incl deleted): %w", err)
	}
	deletedVersions := toMap(deletedVersionsResponse)

	versionsResponse, err := r.client.SubjectVersions(r.Ctx, subject)
	if err != nil && len(deletedVersions) == 0 {
		return nil, nil, fmt.Errorf("unable to retrieve subject versions: %w", err)
	} else {
		versions := toMap(versionsResponse)
		for _, version := range versionsResponse {
			delete(deletedVersions, version)
		}
		return versions, deletedVersions, nil
	}
}

func (r *RestSource) getSubjectSchema(subject string, version int) (*sr.SubjectSchema, error) {
	for i := 0; i < 10; i++ {
		subjectSchema, err := r.client.SchemaByVersion(r.Ctx, subject, version)
		if err == nil {
			return &subjectSchema, err
		} else {
			if i == 9 {
				return nil, err
			}
		}
	}
	panic("oops")
}

func (r *RestSource) GetState() (*State, error) {
	subjectSchemas := make([]sr.SubjectSchema, 0)
	softDeletions := make([]sr.SubjectVersion, 0)

	subjects, deletedSubjects, err := r.getSubjects()

	if err != nil {
		return nil, err
	}

	for subject := range subjects {
		versions, deletedVersions, err := r.getVersions(subject)
		if err != nil {
			return nil, err
		}
		for version := range versions {
			subjectSchema, err := r.getSubjectSchema(subject, version)
			if err != nil {
				return nil, fmt.Errorf("unable to retrieve schemas: %w", err)
			}
			subjectSchemas = append(subjectSchemas, *subjectSchema)
		}
		for version := range deletedVersions {
			subjectSchema, err := r.getSubjectSchema(subject, version)
			if err != nil {
				return nil, fmt.Errorf("unable to retrieve schemas: %w", err)
			}
			subjectSchemas = append(subjectSchemas, *subjectSchema)
			softDeletions = append(softDeletions, sr.SubjectVersion{Subject: subjectSchema.Subject, Version: subjectSchema.Version})
		}
	}

	for deletedSubject := range deletedSubjects {
		versions, deletedVersions, err := r.getVersions(deletedSubject)
		if err != nil {
			return nil, err
		}
		for version := range versions {
			subjectSchema, err := r.getSubjectSchema(deletedSubject, version)
			if err != nil {
				return nil, fmt.Errorf("unable to retrieve schemas: %w", err)
			}
			subjectSchemas = append(subjectSchemas, *subjectSchema)
		}
		for version := range deletedVersions {
			subjectSchema, err := r.getSubjectSchema(deletedSubject, version)
			if err != nil {
				return nil, fmt.Errorf("unable to retrieve schemas: %w", err)
			}
			subjectSchemas = append(subjectSchemas, *subjectSchema)
			softDeletions = append(softDeletions, sr.SubjectVersion{Subject: subjectSchema.Subject, Version: subjectSchema.Version})
		}
	}

	rawCompatibilityResults := r.client.Compatibility(r.Ctx, slices.Collect(maps.Keys(subjects))...)
	prunedCompatibilityResults := filter(rawCompatibilityResults, func(result sr.CompatibilityResult) bool {
		return result.Err == nil || result.Err.(*sr.ResponseError).StatusCode != 404
	})

	errs := make([]error, 0)
	for _, result := range prunedCompatibilityResults {
		if result.Err != nil {
			errs = append(errs, result.Err)
		}
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}

	var result State
	result.SubjectSchemas = subjectSchemas
	result.CompatibilityResults = prunedCompatibilityResults
	result.SoftDeletions = softDeletions

	return &result, nil

}
