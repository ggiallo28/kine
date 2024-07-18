package server

import (
	"context"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// explicit interface check
var _ etcdserverpb.KVServer = (*KVServerBridge)(nil)

func (k *KVServerBridge) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if r.KeysOnly {
		return nil, unsupported("keysOnly")
	}

	if r.MaxCreateRevision != 0 {
		return nil, unsupported("maxCreateRevision")
	}

	if r.SortOrder != 0 {
		return nil, unsupported("sortOrder")
	}

	if r.SortTarget != 0 {
		return nil, unsupported("sortTarget")
	}

	if r.Serializable {
		return nil, unsupported("serializable")
	}

	if r.MinModRevision != 0 {
		return nil, unsupported("minModRevision")
	}

	if r.MinCreateRevision != 0 {
		return nil, unsupported("minCreateRevision")
	}

	if r.MaxCreateRevision != 0 {
		return nil, unsupported("maxCreateRevision")
	}

	if r.MaxModRevision != 0 {
		return nil, unsupported("maxModRevision")
	}

	resp, err := k.limited.Range(ctx, r)
	if err != nil {
		logrus.Errorf("error while range on %s %s: %v", r.Key, r.RangeEnd, err)
		return nil, err
	}

	rangeResponse := &etcdserverpb.RangeResponse{
		More:   resp.More,
		Count:  resp.Count,
		Header: resp.Header,
		Kvs:    toKVs(resp.Kvs...),
	}

	return rangeResponse, nil
}

func toKVs(kvs ...*KeyValue) []*mvccpb.KeyValue {
	if len(kvs) == 0 || kvs[0] == nil {
		return nil
	}

	ret := make([]*mvccpb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		newKV := toKV(kv)
		if newKV != nil {
			ret = append(ret, newKV)
		}
	}
	return ret
}

func toKV(kv *KeyValue) *mvccpb.KeyValue {
	if kv == nil {
		return nil
	}
	return &mvccpb.KeyValue{
		Key:            []byte(kv.Key),
		Value:          kv.Value,
		Lease:          kv.Lease,
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
	}
}

// func (k *KVServerBridge) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
// 	return nil, unsupported("put")
// }

// func (k *KVServerBridge) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
// 	return nil, unsupported("delete")
// }

func (k *KVServerBridge) Put(ctx context.Context, r *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	resp, err := k.limited.Put(ctx, r)
	if err != nil {
		logrus.Errorf("error while putting key %s: %v", r.Key, err)
		return nil, err
	}

	putResponse := &etcdserverpb.PutResponse{
		Header: resp.Header,
		PrevKv: resp.PrevKv,
	}

	return putResponse, nil
}

func (k *KVServerBridge) DeleteRange(ctx context.Context, r *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	resp, err := k.limited.DeleteRange(ctx, r)
	if err != nil {
		logrus.Errorf("error while deleting range %s %s: %v", r.Key, r.RangeEnd, err)
		return nil, err
	}

	deleteResponse := &etcdserverpb.DeleteRangeResponse{
		Header: resp.Header,
		Deleted: resp.Deleted,
		PrevKvs: resp.PrevKvs,
	}

	return deleteResponse, nil
}

// func (k *KVServerBridge) Txn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
// 	res, err := k.limited.Txn(ctx, r)
// 	if err != nil {
// 		logrus.Errorf("error in txn %s: %v", r, err)
// 	}
// 	return res, err
// }

func (k *KVServerBridge) Txn(ctx context.Context, r *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
    for _, c := range r.Compare {
        if c.Target == etcdserverpb.Compare_VALUE {
            return nil, unsupported("compare.Value")
        }
        if c.Target == etcdserverpb.Compare_CREATE {
            return nil, unsupported("compare.Create")
        }
        if c.Target == etcdserverpb.Compare_MOD {
            return nil, unsupported("compare.Mod")
        }
    }

    for _, op := range r.Success {
        if op.GetRequestPut() != nil {
            return nil, unsupported("txn.Success.Put")
        }
        if op.GetRequestDeleteRange() != nil {
            return nil, unsupported("txn.Success.DeleteRange")
        }
    }

    for _, op := range r.Failure {
        if op.GetRequestPut() != nil {
            return nil, unsupported("txn.Failure.Put")
        }
        if op.GetRequestDeleteRange() != nil {
            return nil, unsupported("txn.Failure.DeleteRange")
        }
    }

    res, err := k.limited.Txn(ctx, r)
    if err != nil {
        logrus.Errorf("error in txn %s: %v", r, err)
    }
    return res, err
}


func (k *KVServerBridge) Compact(ctx context.Context, r *etcdserverpb.CompactionRequest) (*etcdserverpb.CompactionResponse, error) {
	res, err := k.limited.Compact(ctx, r)
	if err != nil {
		logrus.Errorf("error in compact %s: %v", r, err)
	}
	return res, err
}