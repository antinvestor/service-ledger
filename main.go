package main

import (
	"context"
	"fmt"
	"github.com/antinvestor/service-ledger/config"
	"github.com/antinvestor/service-ledger/controllers"
	"github.com/antinvestor/service-ledger/ledger"
	"github.com/antinvestor/service-ledger/models"
	_ "github.com/golang-migrate/migrate/source/file"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcrecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpcctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"github.com/pitabwire/frame"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {

	serviceName := "service_ledger"
	ctx := context.Background()

	var ledgerConfig config.LedgerConfig
	err := frame.ConfigProcess("", &ledgerConfig)
	if err != nil {
		logrus.WithError(err).Fatal("could not process configs")
		return
	}

	service := frame.NewService(serviceName, frame.Config(&ledgerConfig), frame.Datastore(ctx))
	log := service.L()

	var serviceOptions []frame.Option
	if ledgerConfig.DoDatabaseMigrate() {
		service.Init(serviceOptions...)

		err := service.MigrateDatastore(ctx,
			ledgerConfig.GetDatabaseMigrationPath(),
			&models.Ledger{}, &models.Account{},
			&models.Transaction{}, &models.TransactionEntry{})

		if err != nil {
			log.Fatalf("main -- Could not migrate successfully because : %+v", err)
		}
		return
	}

	serviceTranslations := frame.Translations("en")
	serviceOptions = append(serviceOptions, serviceTranslations)

	jwtAudience := ledgerConfig.Oauth2JwtVerifyAudience
	if jwtAudience == "" {
		jwtAudience = serviceName
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpcctxtags.UnaryServerInterceptor(),
			grpcrecovery.UnaryServerInterceptor(),
			service.UnaryAuthInterceptor(jwtAudience, ledgerConfig.Oauth2JwtVerifyIssuer),
		)),
		grpc.StreamInterceptor(service.StreamAuthInterceptor(jwtAudience, ledgerConfig.Oauth2JwtVerifyIssuer)),
	)

	implementation := &controllers.LedgerServer{
		Service: service,
	}

	ledger.RegisterLedgerServiceServer(grpcServer, implementation)

	grpcServerOpt := frame.GrpcServer(grpcServer)
	serviceOptions = append(serviceOptions, grpcServerOpt)

	service.Init(serviceOptions...)

	serverPort := ledgerConfig.ServerPort

	log.Info(" Initiating server operations on : %s", serverPort)
	err = service.Run(ctx, fmt.Sprintf(":%v", serverPort))
	if err != nil {
		log.Printf("main -- Could not run Server : %v", err)
	}
}
