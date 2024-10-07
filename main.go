package main

import (
	"fmt"
	"github.com/antinvestor/apis/go/common"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/config"
	"github.com/antinvestor/service-ledger/service/handlers"
	"github.com/antinvestor/service-ledger/service/models"
	"github.com/bufbuild/protovalidate-go"
	_ "github.com/golang-migrate/migrate/source/file"
	protovalidateinterceptor "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/pitabwire/frame"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "net/http/pprof"
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
		return
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			protovalidateinterceptor.UnaryServerInterceptor(validator),
			recovery.UnaryServerInterceptor(recovery.WithRecoveryHandlerContext(frame.RecoveryHandlerFun)),
			service.UnaryAuthInterceptor(jwtAudience, ledgerConfig.Oauth2JwtVerifyIssuer),
		),
		grpc.ChainStreamInterceptor(
			protovalidateinterceptor.StreamServerInterceptor(validator),
			recovery.StreamServerInterceptor(recovery.WithRecoveryHandlerContext(frame.RecoveryHandlerFun)),
			service.StreamAuthInterceptor(jwtAudience, ledgerConfig.Oauth2JwtVerifyIssuer),
		),
	)

	implementation := &handlers.LedgerServer{
		Service: service,
	}

	ledgerV1.RegisterLedgerServiceServer(grpcServer, implementation)

	grpcServerOpt := frame.GrpcServer(grpcServer)
	serviceOptions = append(serviceOptions, grpcServerOpt)

	proxyOptions := common.ProxyOptions{
		GrpcServerEndpoint: fmt.Sprintf("localhost:%s", ledgerConfig.GrpcServerPort),
		GrpcServerDialOpts: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}

	proxyMux, err := ledgerV1.CreateProxyHandler(ctx, proxyOptions)
	if err != nil {
		log.WithError(err).Fatal("could not create the proxy handler")
		return
	}

	proxyServerOpt := frame.HttpHandler(proxyMux)
	serviceOptions = append(serviceOptions, proxyServerOpt)

	service.Init(serviceOptions...)

	log.WithField("server http port", ledgerConfig.HttpServerPort).
		WithField("server grpc port", ledgerConfig.GrpcServerPort).
		Info(" Initiating server operations")

	err = service.Run(ctx, "")
	if err != nil {
		log.Printf("main -- Could not run Server : %v", err)
	}
}
