package main

import (
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/config"
	"github.com/antinvestor/service-ledger/service/handlers"
	"github.com/antinvestor/service-ledger/service/models"
	"github.com/bufbuild/protovalidate-go"
	_ "github.com/golang-migrate/migrate/source/file"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	protovalidateinterceptor "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"

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
	log := service.L(ctx)

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

	validator, err := protovalidate.New()
	if err != nil {
		log.WithError(err).Fatal("could not load validator for proto messages")
	}

	unaryInterceptors := []grpc.UnaryServerInterceptor{
		logging.UnaryServerInterceptor(frame.LoggingInterceptor(log), frame.GetLoggingOptions()...),
		protovalidateinterceptor.UnaryServerInterceptor(validator),
		recovery.UnaryServerInterceptor(recovery.WithRecoveryHandlerContext(frame.RecoveryHandlerFun)),
	}

	if ledgerConfig.SecurelyRunService {
		unaryInterceptors = append([]grpc.UnaryServerInterceptor{service.UnaryAuthInterceptor(jwtAudience, ledgerConfig.Oauth2JwtVerifyIssuer)}, unaryInterceptors...)
	}

	streamInterceptors := []grpc.StreamServerInterceptor{
		logging.StreamServerInterceptor(frame.LoggingInterceptor(log), frame.GetLoggingOptions()...),
		protovalidateinterceptor.StreamServerInterceptor(validator),
		recovery.StreamServerInterceptor(recovery.WithRecoveryHandlerContext(frame.RecoveryHandlerFun)),
	}

	if ledgerConfig.SecurelyRunService {
		streamInterceptors = append([]grpc.StreamServerInterceptor{service.StreamAuthInterceptor(jwtAudience, ledgerConfig.Oauth2JwtVerifyIssuer)}, streamInterceptors...)
	} else {
		log.Warn("service is running insecurely: secure by setting SECURELY_RUN_SERVICE=True")
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
	)

	implementation := &handlers.LedgerServer{
		Service: service,
	}

	ledgerV1.RegisterLedgerServiceServer(grpcServer, implementation)

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
