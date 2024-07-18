package server

import (
	"context"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

type LimitedServer struct {
	notifyInterval time.Duration
	backend        Backend
	scheme         string
}

func (l *LimitedServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	if len(r.RangeEnd) == 0 {
		return l.get(ctx, r)
	}
	return l.list(ctx, r)
}

// Put method to store a key-value pair.
func (l *LimitedServer) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	err := l.backend.Put(ctx, r.Key, r.Value)
	if err != nil {
		return nil, err
	}

	return &etcdserverpb.PutResponse{
		Header: txnHeader(0), // Replace 0 with appropriate revision number
	}, nil
}

// DeleteRange method to delete a range of key-value pairs.
func (l *LimitedServer) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	err := l.backend.DeleteRange(ctx, r.Key, r.RangeEnd)
	if err != nil {
		return nil, err
	}

	return &etcdserverpb.DeleteRangeResponse{
		Header: txnHeader(0), // Replace 0 with appropriate revision number
	}, nil
}

func txnHeader(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: rev,
	}
}

func (l *LimitedServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if put := isCreate(txn); put != nil {
		return l.create(ctx, put)
	}
	if rev, key, ok := isDelete(txn); ok {
		return l.delete(ctx, key, rev)
	}
	if rev, key, value, lease, ok := isUpdate(txn); ok {
		return l.update(ctx, rev, key, value, lease)
	}
	if isCompact(txn) {
		return l.compact()
	}
	return nil, ErrNotSupported
}

type ResponseHeader struct {
	Revision int64
}

type RangeResponse struct {
	Header *etcdserverpb.ResponseHeader
	Kvs    []*KeyValue
	More   bool
	Count  int64
}
