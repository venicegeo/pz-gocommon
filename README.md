# pz-gocommon

The pz-gocommon project is a support library for software projects based on the Go language.

***
## Requirements
Before building and running the pz-gocommon project, please ensure that the following components are available and/or installed, as necessary:
- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) (for checking out repository source)
- [Go](https://golang.org/doc/install) v1.7 or later
- [Glide](https://glide.sh)

For additional details on prerequisites, please refer to the Piazza Developer's Guide repository content for [Core Overview](https://github.com/venicegeo/pz-docs/blob/master/documents/devguide/02-pz-core.md) or [Piazza Job Common](https://github.com/venicegeo/pz-docs/blob/master/documents/devguide/16-job-common.md) sections. Also refer to the [prerequisites for using Piazza](https://github.com/venicegeo/pz-docs/blob/master/documents/devguide/03-jobs.md) section for additional details.

***
## Setup

Create the directory the repository must live in, then clone the repository there:

    $ mkdir -p $GOPATH/src/github.com/venicegeo
    $ cd $GOPATH/src/github.com/venicegeo
    $ git clone git@github.com:venicegeo/pz-gocommon.git
    $ cd pz-gocommon

Set up Go environment variables

To function right, Go must have some environment variables set. Run the `go env`
command to list all relevant environment variables. The two most important 
variables to look for are `GOROOT` and `GOPATH`.

- `GOROOT` must point to the base directory at which Go is installed
- `GOPATH` must point to a directory that is to serve as your development
  environment. This is where this code and dependencies will live.

To quickly verify these variables are set, run the command from terminal:

```
go env | egrep "GOPATH|GOROOT"
```

## Installing, Building, Running & Unit Tests

### Install dependencies

This project manages dependencies by populating a `vendor/` directory using the
glide tool. If the tool is already installed, in the code repository, run:

    $ glide install -v

This will retrieve all the relevant dependencies at their appropriate versions
and place them in `vendor/`, which enables Go to use those versions in building
rather than the default (which is the newest revision in Github).

> **Adding new dependencies.** When adding new dependencies, simply installing
  them with `go get <package>` will fetch their latest version and place it in
  `$GOPATH/src`. This is undesirable, since it is not repeatable for others.
  Instead, to add a dependency, use `glide get <package>`, which will place it
  in `vendor/` and update `glide.yaml` and `glide.lock` to remember its version.
  
### Build the project
To build `pz-gocommon`, run `go install` from the project directory. To build it from elsewhere, run:

	$ go get github.com/venicegeo/pz-gocommon/gocommon

This will build the pz-gocommon source

### Run unit tests with coverage collection

Run pz-gocommon tests with coverage collection:

	$ go test -v -coverprofile=gocommon.cov github.com/venicegeo/pz-gocommon/gocommon
  
Run elasticsearch tests with coverage collection:

	$ go test -v -coverprofile=elasticsearch.cov github.com/venicegeo/pz-gocommon/elasticsearch
	
Run kafka tests with coverage collection:

	$ go test -v -coverprofile=kafka.cov github.com/venicegeo/pz-gocommon/kafka
	
Run syslog tests with coverage collection:

	$ go test -v -coverprofile=syslog.cov github.com/venicegeo/pz-gocommon/syslog  
	
