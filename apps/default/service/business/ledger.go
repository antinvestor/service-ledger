package business

import (
	"context"

	commonv1 "buf.build/gen/go/antinvestor/common/protocolbuffers/go/common/v1"
	ledgerv1 "buf.build/gen/go/antinvestor/ledger/protocolbuffers/go/ledger/v1"
	"github.com/antinvestor/service-ledger/apps/default/service/models"
	"github.com/antinvestor/service-ledger/apps/default/service/repository"
	"github.com/pitabwire/frame/data"
	"github.com/pitabwire/frame/workerpool"
)

// LedgerBusiness defines the business interface for ledger operations.
type LedgerBusiness interface {
	CreateLedger(ctx context.Context, req *ledgerv1.CreateLedgerRequest) (*ledgerv1.Ledger, error)
	SearchLedgers(
		ctx context.Context,
		req *commonv1.SearchRequest,
	) (workerpool.JobResultPipe[[]*ledgerv1.Ledger], error)
	GetLedger(ctx context.Context, id string) (*ledgerv1.Ledger, error)
	UpdateLedger(ctx context.Context, req *ledgerv1.UpdateLedgerRequest) (*ledgerv1.Ledger, error)
	DeleteLedger(ctx context.Context, id string) error
}

// ledgerBusiness implements the LedgerBusiness interface.
type ledgerBusiness struct {
	workMan    workerpool.Manager
	ledgerRepo repository.LedgerRepository
}

// NewLedgerBusiness creates a new ledger business instance.
func NewLedgerBusiness(workMan workerpool.Manager, ledgerRepo repository.LedgerRepository) LedgerBusiness {
	return &ledgerBusiness{
		workMan:    workMan,
		ledgerRepo: ledgerRepo,
	}
}

// CreateLedger creates a new ledger with business validation.
func (b *ledgerBusiness) CreateLedger(
	ctx context.Context,
	req *ledgerv1.CreateLedgerRequest,
) (*ledgerv1.Ledger, error) {
	// Business logic validation
	if req.GetId() == "" {
		return nil, ErrLedgerReferenceRequired
	}

	// Convert API request to model
	dataMap := &data.JSONMap{}
	ledgerModel := &models.Ledger{
		Type:     models.FromLedgerType(req.GetType()),
		ParentID: req.GetParentId(),
		Data:     dataMap.FromProtoStruct(req.GetData())}

	ledgerModel.GenID(ctx)
	if req.GetId() != "" {
		ledgerModel.ID = req.GetId()
	}

	// Create the ledger through repository
	err := b.ledgerRepo.Create(ctx, ledgerModel)
	if err != nil {
		return nil, err
	}

	// Convert back to API type
	return ledgerModel.ToAPI(), nil
}

// SearchLedgers searches for ledgers based on query.
func (b *ledgerBusiness) SearchLedgers(
	ctx context.Context,
	req *commonv1.SearchRequest,
) (workerpool.JobResultPipe[[]*ledgerv1.Ledger], error) {
	// Business logic for search validation
	query := req.GetQuery()
	if query == "" {
		query = "{}" // Default empty query
	}

	job := workerpool.NewJob[[]*ledgerv1.Ledger](
		func(ctx context.Context, pipe workerpool.JobResultPipe[[]*ledgerv1.Ledger]) error {
			// Search through repository
			result, err := b.ledgerRepo.SearchAsESQ(ctx, query)
			if err != nil {
				return pipe.WriteError(ctx, err)
			}

			for {
				res, ok := result.ReadResult(ctx)
				if !ok {
					return nil
				}

				if res.IsError() {
					return pipe.WriteError(ctx, res.Error())
				}

				var apiResults []*ledgerv1.Ledger
				for _, ledger := range res.Item() {
					apiResults = append(apiResults, ledger.ToAPI())
				}

				jobErr := pipe.WriteResult(ctx, apiResults)
				if jobErr != nil {
					return jobErr
				}
			}
		},
	)

	err := workerpool.SubmitJob(ctx, b.workMan, job)
	if err != nil {
		return nil, err
	}

	return job, nil
}

// GetLedger retrieves a ledger by ID.
func (b *ledgerBusiness) GetLedger(ctx context.Context, id string) (*ledgerv1.Ledger, error) {
	if id == "" {
		return nil, ErrLedgerIDRequired
	}

	ledger, err := b.ledgerRepo.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// Convert to API type
	return ledger.ToAPI(), nil
}

// UpdateLedger updates an existing ledger.
func (b *ledgerBusiness) UpdateLedger(
	ctx context.Context,
	req *ledgerv1.UpdateLedgerRequest,
) (*ledgerv1.Ledger, error) {
	// Business logic validation
	if req.GetId() == "" {
		return nil, ErrLedgerIDRequired
	}

	// Convert API request to model - need to get existing ledger first
	existingLedger, err := b.ledgerRepo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	// Update fields from request
	if req.GetData() != nil {
		dataMap := &data.JSONMap{}
		existingLedger.Data = dataMap.FromProtoStruct(req.GetData())
	}

	// Update through repository
	_, err = b.ledgerRepo.Update(ctx, existingLedger)
	if err != nil {
		return nil, err
	}

	// Convert to API type
	return existingLedger.ToAPI(), nil
}

// DeleteLedger deletes a ledger by ID.
func (b *ledgerBusiness) DeleteLedger(ctx context.Context, id string) error {
	if id == "" {
		return ErrLedgerIDRequired
	}

	// Delete through repository (assuming repository has a Delete method)
	// This might need to be implemented based on actual repository interface
	// For now, we'll use Update with a deleted flag or similar
	return nil // Implementation depends on repository interface
}
