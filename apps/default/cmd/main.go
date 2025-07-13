package main

import (
	"buf.build/go/protovalidate"
	"context"
	"fmt"
	"github.com/antinvestor/apis/go/common"
	ledgerV1 "github.com/antinvestor/apis/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/config"
	"github.com/antinvestor/service-ledger/apps/default/service/handlers"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	protovalidateinterceptor "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/pitabwire/frame"
	"github.com/pitabwire/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "net/http/pprof"
)

func main() {

	serviceName := "service_ledger"
	ctx := context.Background()

	cfg, err := frame.ConfigFromEnv[config.LedgerConfig]()
	if err != nil {
		util.Log(ctx).WithError(err).Fatal("could not process configs")
		return
	}

	ctx, svc := frame.NewServiceWithContext(ctx, serviceName, frame.WithConfig(&cfg))
	defer svc.Stop(ctx)
	log := svc.Log(ctx)

	serviceOptions := []frame.Option{frame.WithDatastore()}

	// Handle database migration if requested
	if handleDatabaseMigration(ctx, svc, cfg, log) {
		return
	}

	serviceTranslations := frame.WithTranslations("en")
	serviceOptions = append(serviceOptions, serviceTranslations)

	jwtAudience := cfg.Oauth2JwtVerifyAudience
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
			svc.UnaryAuthInterceptor(jwtAudience, cfg.Oauth2JwtVerifyIssuer),
		),
		grpc.ChainStreamInterceptor(
			protovalidateinterceptor.StreamServerInterceptor(validator),
			recovery.StreamServerInterceptor(recovery.WithRecoveryHandlerContext(frame.RecoveryHandlerFun)),
			svc.StreamAuthInterceptor(jwtAudience, cfg.Oauth2JwtVerifyIssuer),
		),
	)

	implementation := &handlers.LedgerServer{
		Service: svc,
	}

	ledgerV1.RegisterLedgerServiceServer(grpcServer, implementation)

	grpcServerOpt := frame.WithGRPCServer(grpcServer)
	serviceOptions = append(serviceOptions, grpcServerOpt)

	proxyOptions := common.ProxyOptions{
		GrpcServerEndpoint: fmt.Sprintf("localhost:%s", cfg.GrpcServerPort),
		GrpcServerDialOpts: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	}

	proxyMux, err := ledgerV1.CreateProxyHandler(ctx, proxyOptions)
	if err != nil {
		log.WithError(err).Fatal("could not create the proxy handler")
		return
	}

	proxyServerOpt := frame.WithHTTPHandler(proxyMux)
	serviceOptions = append(serviceOptions, proxyServerOpt)

	svc.Init(ctx, serviceOptions...)

	log.WithField("server http port", cfg.HTTPServerPort).
		WithField("server grpc port", cfg.GrpcServerPort).
		Info(" Initiating server operations")

	err = svc.Run(ctx, "")
	if err != nil {
		log.Printf("main -- Could not run Server : %v", err)
	}
}

// handleDatabaseMigration performs database migration if configured to do so.
func handleDatabaseMigration(
	ctx context.Context,
	svc *frame.Service,
	cfg config.LedgerConfig,
	log *util.LogEntry,
) bool {
	serviceOptions := []frame.Option{frame.WithDatastore()}

	if cfg.DoDatabaseMigrate() {
		svc.Init(ctx, serviceOptions...)

		err := repository.Migrate(ctx, svc, cfg.GetDatabaseMigrationPath())
		if err != nil {
			log.WithError(err).Fatal("main -- Could not migrate successfully")
		}
		return true
	}
	return false
}
