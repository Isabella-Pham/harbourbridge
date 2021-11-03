// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package postgres_test

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/conversion"
	"github.com/cloudspannerecosystem/harbourbridge/testing/common"
	"google.golang.org/api/iterator"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	storage "cloud.google.com/go/storage"
	"google.golang.org/api/option"
)

var (
	projectID  string
	instanceID string

	ctx           context.Context
	databaseAdmin *database.DatabaseAdminClient
)

func TestMain(m *testing.M) {
	cleanup := initIntegrationTests()
	res := m.Run()
	cleanup()
	os.Exit(res)
}

func initIntegrationTests() (cleanup func()) {
	projectID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID")
	instanceID = os.Getenv("HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID")

	ctx = context.Background()
	flag.Parse() // Needed for testing.Short().
	noop := func() {}

	if testing.Short() {
		log.Println("Integration tests skipped in -short mode.")
		return noop
	}

	if projectID == "" {
		log.Println("Integration tests skipped: HARBOURBRIDGE_TESTS_GCLOUD_PROJECT_ID is missing")
		return noop
	}

	if instanceID == "" {
		log.Println("Integration tests skipped: HARBOURBRIDGE_TESTS_GCLOUD_INSTANCE_ID is missing")
		return noop
	}

	var err error
	databaseAdmin, err = database.NewDatabaseAdminClient(ctx)
	if err != nil {
		log.Fatalf("cannot create databaseAdmin client: %v", err)
	}

	return func() {
		databaseAdmin.Close()
	}
}

func dropDatabase(t *testing.T, dbURI string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// Drop the testing database.
	if err := databaseAdmin.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbURI}); err != nil {
		t.Fatalf("failed to drop testing database %v: %v", dbURI, err)
	}
}

func prepareIntegrationTest(t *testing.T) string {
	if databaseAdmin == nil {
		t.Skip("Integration tests skipped")
	}
	tmpdir, err := ioutil.TempDir(".", "int-test-")
	if err != nil {
		log.Fatal(err)
	}
	return tmpdir
}

func TestIntegration_PGDUMP_Command(t *testing.T) {
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(constants.PGDUMP, now)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)

	dataFilepath := "../../test_data/pg_dump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName+".")

	args := fmt.Sprintf("-prefix %s -instance %s -dbname %s < %s", filePrefix, instanceID, dbName, dataFilepath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI)
}

func TestIntegration_PGDUMP_EvalSubcommand(t *testing.T) {
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(constants.PGDUMP, now)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)

	dataFilepath := "../../test_data/pg_dump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName+".")

	args := fmt.Sprintf("eval -prefix %s -source=postgres -target-profile='instance=%s,dbname=%s' < %s", filePrefix, instanceID, dbName, dataFilepath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI)
}

func TestIntegration_PGDUMP_SchemaSubcommand(t *testing.T) {
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	dataFilepath := "../../test_data/pg_dump.test.out"

	args := fmt.Sprintf("schema -source=pg < %s", dataFilepath)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_POSTGRES_Command(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(constants.POSTGRES, now)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName+".")

	args := fmt.Sprintf("-instance %s -dbname %s -prefix %s -driver %s", instanceID, dbName, filePrefix, constants.POSTGRES)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI)
}

func TestIntegration_POSTGRES_EvalSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(constants.POSTGRES, now)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName+".")

	args := fmt.Sprintf("eval -prefix %s -source=postgres -target-profile='instance=%s,dbname=%s'", filePrefix, instanceID, dbName)
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI)
}

func TestIntegration_POSTGRES_SchemaSubcommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	args := "schema -source=postgres"
	err := common.RunCommand(args, projectID)
	if err != nil {
		t.Fatal(err)
	}
}

func TestIntegration_PGDUMP_Command_GCS(t *testing.T) {
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)
	createGCSBucketWithDumpfile()

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(conversion.PGDUMP, now)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)

	dataFilepath := "gs://test-bucket/pg_dump.test.out"
	filePrefix := filepath.Join(tmpdir, dbName+".")
	// Be aware that when testing with the command, the time `now` might be
	// different between file prefixes and the contents in the files. This
	// is because file prefixes use `now` from here (the test function) and
	// the generated time in the files uses a `now` inside the command, which
	// can be different.
	cmd := exec.Command("bash", "-c", fmt.Sprintf("go run github.com/cloudspannerecosystem/harbourbridge -instance %s -dbname %s -prefix %s < %s", instanceID, dbName, filePrefix, dataFilepath))
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GCLOUD_PROJECT=%s", projectID),
	)
	if err := cmd.Run(); err != nil {
		fmt.Printf("stdout: %q\n", out.String())
		fmt.Printf("stderr: %q\n", stderr.String())
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI)
}

func TestIntegration_PGDUMP_SchemaCommand_GCS(t *testing.T) {
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)
	createGCSBucketWithDumpfile()

	dataFilepath := "gs://test-bucket/pg_dump.test.out"
	// Be aware that when testing with the command, the time `now` might be
	// different between file prefixes and the contents in the files. This
	// is because file prefixes use `now` from here (the test function) and
	// the generated time in the files uses a `now` inside the command, which
	// can be different.
	cmd := exec.Command("bash", "-c", fmt.Sprintf("go run github.com/cloudspannerecosystem/harbourbridge schema -source=pg < %s", dataFilepath))
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("stdout: %q\n", out.String())
		fmt.Printf("stderr: %q\n", stderr.String())
		t.Fatal(err)
	}
}

func createGCSBucketWithDumpfile(){
	client, err := storage.NewClient(ctx, option.WithEndpoint("http://localhost:9000/storage/v1/"))
    if err != nil {
        log.Fatal(err)
    }
	defer client.Close()
    // This request is directed to http://localhost:9000/storage/v1/
    // instead of https://storage.googleapis.com/storage/v1/
	bucketName := "test-bucket"
	err = client.Bucket(bucketName).Create(ctx, projectID, nil)
    if err != nil {
        log.Fatal(err)
    }

	bkt := client.Bucket(bucketName)
	obj := bkt.Object("pg_dump.test.out")

	localDumpFilePath := "../../test_data/pg_dump.test.out"
	localDumpFile, err := os.Open(localDumpFilePath)
	if err != nil {
        log.Fatal(err)
    }

	dumpFile, err := ioutil.ReadAll(localDumpFile)
	if err != nil {
        log.Fatal(err)
    }

    // Upload dump file to bucket
    w := obj.NewWriter(ctx)
	_, err = fmt.Fprintf(w, string(dumpFile))
    if err != nil {
        log.Fatal(err)
    }

<<<<<<< HEAD
    if err := w.Close(); err != nil {
        log.Fatal(err)
    }
	
=======
	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	now := time.Now()
	dbName, _ := conversion.GetDatabaseName(conversion.POSTGRES, now)
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, dbName)
	filePrefix := filepath.Join(tmpdir, dbName+".")

	cmd := exec.Command("bash", "-c", fmt.Sprintf("go run github.com/cloudspannerecosystem/harbourbridge -instance %s -dbname %s -prefix %s -driver %s", instanceID, dbName, filePrefix, conversion.POSTGRES))
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("GCLOUD_PROJECT=%s", projectID),
	)
	if err := cmd.Run(); err != nil {
		fmt.Printf("stdout: %q\n", out.String())
		fmt.Printf("stderr: %q\n", stderr.String())
		t.Fatal(err)
	}
	// Drop the database later.
	defer dropDatabase(t, dbURI)

	checkResults(t, dbURI)
}

func TestIntegration_POSTGRES_SchemaCommand(t *testing.T) {
	onlyRunForEmulatorTest(t)
	t.Parallel()

	tmpdir := prepareIntegrationTest(t)
	defer os.RemoveAll(tmpdir)

	cmd := exec.Command("bash", "-c", "go run github.com/cloudspannerecosystem/harbourbridge schema -source=postgres")
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("stdout: %q\n", out.String())
		fmt.Printf("stderr: %q\n", stderr.String())
		t.Fatal(err)
	}
>>>>>>> 6522c9b (Add support for source-profile and target-profile in subcommands. (#208))
}

func checkResults(t *testing.T, dbURI string) {
	// Make a query to check results.
	client, err := spanner.NewClient(ctx, dbURI)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	checkBigInt(ctx, t, client)
	checkTimestamps(ctx, t, client)
	checkCoreTypes(ctx, t, client)
	checkArrays(ctx, t, client)
}

func checkBigInt(ctx context.Context, t *testing.T, client *spanner.Client) {
	var quantity int64
	iter := client.Single().Read(ctx, "cart", spanner.Key{"31ad80e3-182b-42b0-a164-b4c7ea976ce4", "OLJCESPC7Z"}, []string{"quantity"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&quantity); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := quantity, int64(125); got != want {
		t.Fatalf("quantities are not correct: got %v, want %v", got, want)
	}
}

func checkTimestamps(ctx context.Context, t *testing.T, client *spanner.Client) {
	var ts, tsWithZone time.Time
	iter := client.Single().Read(ctx, "test", spanner.Key{4}, []string{"t", "tz"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&ts, &tsWithZone); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := ts.Format(time.RFC3339Nano), "2019-10-28T15:00:00.123457Z"; got != want {
		t.Fatalf("timestamp is not correct: got %v, want %v", got, want)
	}
	if got, want := tsWithZone.Format(time.RFC3339Nano), "2019-10-28T15:00:00.123457Z"; got != want {
		t.Fatalf("timestamp with time zone is not correct: got %v, want %v", got, want)
	}
}

func checkCoreTypes(ctx context.Context, t *testing.T, client *spanner.Client) {
	var boolVal bool
	var bytesVal []byte
	var date spanner.NullDate
	var floatVal float64
	var intVal int64
	var numericVal big.Rat
	var stringVal string
	iter := client.Single().Read(ctx, "test2", spanner.Key{1}, []string{"a", "b", "c", "d", "e", "f", "g"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&boolVal, &bytesVal, &date, &floatVal, &intVal, &numericVal, &stringVal); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := boolVal, true; got != want {
		t.Fatalf("bool value is not correct: got %v, want %v", got, want)
	}
	if got, want := string(bytesVal), "\x00\x01\x02\x03ޭ\xbe\xef"; got != want {
		t.Fatalf("bytes are not correct: got %v, want %v", got, want)
	}
	if got, want := date.String(), "2019-10-28"; got != want {
		t.Fatalf("date is not correct: got %v, want %v", got, want)
	}
	if got, want := floatVal, 99.9; got != want {
		t.Fatalf("float value is not correct: got %v, want %v", got, want)
	}
	if got, want := intVal, int64(42); got != want {
		t.Fatalf("int value is not correct: got %v, want %v", got, want)
	}
	if got, want := spanner.NumericString(&numericVal), "1234567890123456789012345678.123456789"; got != want {
		t.Fatalf("numeric value is not correct: got %v, want %v", got, want)
	}
	if got, want := stringVal, "hi"; got != want {
		t.Fatalf("string value is not correct: got %v, want %v", got, want)
	}
}

func checkArrays(ctx context.Context, t *testing.T, client *spanner.Client) {
	var ints []int64
	var strs []string
	iter := client.Single().Read(ctx, "test3", spanner.Key{1}, []string{"a", "b"})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := row.Columns(&ints, &strs); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := ints, []int64{1, 2, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("integer array is not correct: got %v, want %v", got, want)
	}
	if got, want := strs, []string{"1", "nice", "foo"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("string array is not correct: got %v, want %v", got, want)
	}
}

func onlyRunForEmulatorTest(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("Skipping tests only running against the emulator.")
	}
}
