// SPDX-License-Identifier: AGPL-3.0

package watcher

import (
	"context"
	"testing"

	"golang.org/x/sync/errgroup"

	"github.com/go-openapi/testify/v2/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/netbirdio/kube-egress-forwarder/pkg/forwarder"
)

func TestWatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	group, ctx := errgroup.WithContext(ctx)
	fwd := forwarder.NewForwarder(ctx)
	group.Go(func() error {
		return fwd.Wait()
	})
	kubeClient := fake.NewSimpleClientset()
	group.Go(func() error {
		return Watch(ctx, fwd, kubeClient, "forwarder", "default")
	})

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "forwarder",
			Namespace: "default",
		},
	}
	_, err := kubeClient.CoreV1().ConfigMaps(cm.Namespace).Create(ctx, cm, metav1.CreateOptions{})
	require.NoError(t, err)

	cancel()
	err = group.Wait()
	require.NoError(t, err)
}
