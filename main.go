package main

import (
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

	var ledgerConfig config.LedgerConfig
	err := frame.ConfigProcess("", &ledgerConfig)
	if err != nil {
		logrus.WithError(err).Fatal("could not process configs")
		return
	}

	ctx, service := frame.NewService(serviceName, frame.Config(&ledgerConfig))
	defer service.Stop(ctx)
	log := service.L()

	serviceOptions := []frame.Option{frame.Datastore(ctx)}
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

	log.WithField("server http port", ledgerConfig.HttpServerPort).
		WithField("server grpc port", ledgerConfig.GrpcServerPort).
		Info(" Initiating server operations")

	err = service.Run(ctx, "")
	if err != nil {
		log.Printf("main -- Could not run Server : %v", err)
	}
}
