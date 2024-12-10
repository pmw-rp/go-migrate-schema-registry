package main

import "github.com/twmb/franz-go/pkg/sr"

func getReference(subjectSchema sr.SubjectSchema) sr.SubjectVersion {
	return sr.SubjectVersion{Subject: subjectSchema.Subject, Version: subjectSchema.Version}
}
