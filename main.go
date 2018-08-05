package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/golang-migrate/migrate"
	"github.com/golang-migrate/migrate/database"
	"github.com/golang-migrate/migrate/database/postgres"
	_ "github.com/golang-migrate/migrate/source/file"
	"google.golang.org/grpc"
	"github.com/facebookgo/inject"
	"net"
	"bitbucket.org/caricah/service-ledger/controllers"
	"bitbucket.org/caricah/service-ledger/ledger"
	"fmt"
	"bitbucket.org/caricah/service-ledger/middlewares"
)

func main() {
	// Assert authentication
	authToken, ok := os.LookupEnv("LEDGER_AUTH_TOKEN")
	if !ok || authToken == "" {
		log.Fatal("Cannot start the server. Authentication token is not set!!")
	}

	db, err := sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Panic("Unable to connect to Database:", err)
	}
	log.Println("Successfully established connection to database.")

	// Migrate DB changes
	migrateDB(db)

	implementation := &controllers.LedgerServer{}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(middlewares.AuthInterceptor),
	)
	ledger.RegisterLedgerServiceServer(grpcServer, implementation)

	graph := inject.Graph{}

	if err = graph.Provide(
		&inject.Object{Name: "db", Value: db},
		&inject.Object{Value: implementation}); nil != err {
		log.Panic(err)
	}

	if err = graph.Populate(); nil != err {
		log.Panic(err)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "7000"
	}

	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		log.Panicf("Could not start on supplied port %v %v ", port, err)
	}

	log.Println("Running server on port:", port)

	// start the server
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}

	defer func() {
		if r := recover(); r != nil {
			log.Println("Server exited!!!", r)
		}
	}()
}

func migrateDB(db *sql.DB) {
	log.Println("Starting db schema migration...")
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		log.Panic("Unable to create database instance for migration:", err)
	}

	migrationFilesPath := os.Getenv("MIGRATION_FILES_PATH")
	if migrationFilesPath == "" {
		migrationFilesPath = "file://migrations/postgres"
	}
	m, err := migrate.NewWithDatabaseInstance(
		migrationFilesPath,
		"postgres", driver)
	if err != nil {
		log.Panic("Unable to create Migrate instance for database:", err)
	}

	version, dirty, err := m.Version()
	if err != nil && err != migrate.ErrNilVersion {
		log.Panic("Unable to get existing migration version for database:", dirty, err)
	}
	log.Println("Current schema version:", version)
	err = m.Up()
	if err != nil {
		switch err {
		case migrate.ErrNoChange:
			log.Println("No changes to migrate")
		case migrate.ErrLocked, database.ErrLocked:
			log.Println("Database locked. Skipping migration assuming another instance working on it")
		default:
			log.Panic("Error while migration:", err)
		}
	}
	version, dirty, err = m.Version()
	if err != nil {
		log.Panic("Unable to get new migration version for database:", dirty, err)
	}
	log.Println("Migrated schema version:", version)
}
