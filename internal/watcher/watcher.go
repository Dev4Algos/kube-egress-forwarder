// SPDX-License-Identifier: AGPL-3.0

package watcher

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apimachinerywatch "k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/watch"

	"github.com/netbirdio/kube-egress-forwarder/pkg/forwarder"
)

func Watch(ctx context.Context, fwd *forwarder.Forwarder, kubeClient kubernetes.Interface, cmName, cmNamespace string) error {
	lw := &cache.ListWatch{
		ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			options.FieldSelector = fmt.Sprintf("metadata.name=%s", cmName)
			return kubeClient.CoreV1().ConfigMaps(cmNamespace).List(ctx, options)
		},
		WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (apimachinerywatch.Interface, error) {
			options.FieldSelector = fmt.Sprintf("metadata.name=%s", cmName)
			return kubeClient.CoreV1().ConfigMaps(cmNamespace).Watch(ctx, options)
		},
	}
	watcher, err := watch.NewRetryWatcherWithContext(ctx, "1", lw)
	if err != nil {
		return err
	}
	for event := range watcher.ResultChan() {
		switch event.Type {
		case apimachinerywatch.Error:
			return kerrors.FromObject(event.Object)
		case apimachinerywatch.Deleted:
			err = fwd.Reconcile(nil)
			if err != nil {
				return err
			}
			return err
		default:
			cm, ok := event.Object.(*corev1.ConfigMap)
			if !ok {
				continue
			}
			ruleMgr, err := forwarder.NewRuleManager(cm.Data)
			if err != nil {
				return err
			}
			err = fwd.Reconcile(ruleMgr.AllRules())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
