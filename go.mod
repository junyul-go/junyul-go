module github.com/junyul-go/junyul-go

go 1.22

require github.com/oklog/ulid/v2 v2.1.1

retract v1.0.1 // SDKVersion constant was not bumped; use v1.0.2 or later.
