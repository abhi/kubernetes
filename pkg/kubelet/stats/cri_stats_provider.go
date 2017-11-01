/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stats

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	cadvisorfs "github.com/google/cadvisor/fs"

	cadvisorapiv2 "github.com/google/cadvisor/info/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	internalapi "k8s.io/kubernetes/pkg/kubelet/apis/cri"
	runtimeapi "k8s.io/kubernetes/pkg/kubelet/apis/cri/v1alpha1/runtime"
	statsapi "k8s.io/kubernetes/pkg/kubelet/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
)

// criStatsProvider implements the containerStatsProvider interface by getting
// the container stats from CRI.
type criStatsProvider struct {
	// cadvisor is used to get the node root filesystem's stats (such as the
	// capacity/available bytes/inodes) that will be populated in per container
	// filesystem stats.
	cadvisor cadvisor.Interface
	// resourceAnalyzer is used to get the volume stats of the pods.
	resourceAnalyzer stats.ResourceAnalyzer
	// runtimeService is used to get the status and stats of the pods and its
	// managed containers.
	runtimeService internalapi.RuntimeService
	// imageService is used to get the stats of the image filesystem.
	imageService internalapi.ImageManagerService
}

// newCRIStatsProvider returns a containerStatsProvider implementation that
// provides container stats using CRI.
func newCRIStatsProvider(
	cadvisor cadvisor.Interface,
	resourceAnalyzer stats.ResourceAnalyzer,
	runtimeService internalapi.RuntimeService,
	imageService internalapi.ImageManagerService,
) containerStatsProvider {
	return &criStatsProvider{
		cadvisor:         cadvisor,
		resourceAnalyzer: resourceAnalyzer,
		runtimeService:   runtimeService,
		imageService:     imageService,
	}
}

// ListPodStats returns the stats of all the pod-managed containers.
func (p *criStatsProvider) ListPodStats() ([]statsapi.PodStats, error) {
	// Gets node root filesystem information, which will be used to populate
	// the available and capacity bytes/inodes in container stats.
	rootFsInfo, err := p.cadvisor.RootFsInfo()
	if err != nil {
		return nil, fmt.Errorf("failed to get rootFs info: %v", err)
	}

	// Creates container map.
	containerMap := make(map[string]*runtimeapi.Container)
	containers, err := p.runtimeService.ListContainers(&runtimeapi.ContainerFilter{})
	if err != nil {
		return nil, fmt.Errorf("failed to list all containers: %v", err)
	}
	for _, c := range containers {
		containerMap[c.Id] = c
	}

	// Creates pod sandbox map.
	podSandboxMap := make(map[string]*runtimeapi.PodSandbox)
	podSandboxes, err := p.runtimeService.ListPodSandbox(&runtimeapi.PodSandboxFilter{})
	if err != nil {
		return nil, fmt.Errorf("failed to list all pod sandboxes: %v", err)
	}
	for _, s := range podSandboxes {
		podSandboxMap[s.Id] = s
	}

	// uuidToFsInfo is a map from filesystem UUID to its stats. This will be
	// used as a cache to avoid querying cAdvisor for the filesystem stats with
	// the same UUID many times.
	uuidToFsInfo := make(map[runtimeapi.StorageIdentifier]*cadvisorapiv2.FsInfo)

	// sandboxIDToPodStats is a temporary map from sandbox ID to its pod stats.
	sandboxIDToPodStats := make(map[string]*statsapi.PodStats)

	resp, err := p.runtimeService.ListContainerStats(&runtimeapi.ContainerStatsFilter{})
	if err != nil {
		return nil, fmt.Errorf("failed to list all container stats: %v", err)
	}

	glog.Infof("Cadvisor stats: {%+v}", caInfos)
	caInfos, err := getCRICadvisorStats(p.cadvisor)
	if err != nil {
		return nil, fmt.Errorf("failed to get container info from cadvisor: %v", err)
	}

	for _, stats := range resp {
		containerID := stats.Attributes.Id
		container, found := containerMap[containerID]
		if !found {
			glog.Errorf("Unknown id %q in container map.", containerID)
			continue
		}

		caStats, found := caInfos[containerID]
		if !found {
			glog.Errorf("Unable to find cadvisor stats for %q", containerID)
			continue
		}

		podSandboxID := container.PodSandboxId
		podSandbox, found := podSandboxMap[podSandboxID]
		if !found {
			glog.Errorf("Unknown id %q in pod sandbox map.", podSandboxID)
			continue
		}

		// Creates the stats of the pod (if not created yet) which the
		// container belongs to.
		ps, found := sandboxIDToPodStats[podSandboxID]
		if !found {
			caPodSandbox, found := caInfos[podSandboxID]
			if !found {
				glog.Errorf("Unable to find cadvisor stats for sandbox %q", podSandboxID)
				continue
			}
			ps = p.makePodStats(podSandbox, caPodSandbox)
			sandboxIDToPodStats[podSandboxID] = ps
		}
		ps.Containers = append(ps.Containers, *p.makeContainerStats(stats, container, &rootFsInfo, uuidToFsInfo, caStats))
	}

	result := make([]statsapi.PodStats, 0, len(sandboxIDToPodStats))
	for _, s := range sandboxIDToPodStats {
		result = append(result, *s)
	}
	return result, nil
}

// ImageFsStats returns the stats of the image filesystem.
func (p *criStatsProvider) ImageFsStats() (*statsapi.FsStats, error) {
	resp, err := p.imageService.ImageFsInfo()
	if err != nil {
		return nil, err
	}

	// CRI may return the stats of multiple image filesystems but we only
	// return the first one.
	//
	// TODO(yguo0905): Support returning stats of multiple image filesystems.
	for _, fs := range resp {
		s := &statsapi.FsStats{
			Time:       metav1.NewTime(time.Unix(0, fs.Timestamp)),
			UsedBytes:  &fs.UsedBytes.Value,
			InodesUsed: &fs.InodesUsed.Value,
		}
		imageFsInfo := p.getFsInfo(fs.StorageId)
		if imageFsInfo != nil {
			// The image filesystem UUID is unknown to the local node or
			// there's an error on retrieving the stats. In these cases, we
			// omit those stats and return the best-effort partial result. See
			// https://github.com/kubernetes/heapster/issues/1793.
			s.AvailableBytes = &imageFsInfo.Available
			s.CapacityBytes = &imageFsInfo.Capacity
			s.InodesFree = imageFsInfo.InodesFree
			s.Inodes = imageFsInfo.Inodes
		}
		return s, nil
	}

	return nil, fmt.Errorf("imageFs information is unavailable")
}

// getFsInfo returns the information of the filesystem with the specified
// storageID. If any error occurs, this function logs the error and returns
// nil.
func (p *criStatsProvider) getFsInfo(storageID *runtimeapi.StorageIdentifier) *cadvisorapiv2.FsInfo {
	if storageID == nil {
		glog.V(2).Infof("Failed to get filesystem info: storageID is nil.")
		return nil
	}
	fsInfo, err := p.cadvisor.GetFsInfoByFsUUID(storageID.Uuid)
	if err != nil {
		msg := fmt.Sprintf("Failed to get the info of the filesystem with id %q: %v.", storageID.Uuid, err)
		if err == cadvisorfs.ErrNoSuchDevice {
			glog.V(2).Info(msg)
		} else {
			glog.Error(msg)
		}
		return nil
	}
	return &fsInfo
}

func (p *criStatsProvider) makePodStats(
	podSandbox *runtimeapi.PodSandbox,
	caPodSandbox *cadvisorapiv2.ContainerInfo,
) *statsapi.PodStats {
	s := &statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name:      podSandbox.Metadata.Name,
			UID:       podSandbox.Metadata.Uid,
			Namespace: podSandbox.Metadata.Namespace,
		},
		// The StartTime in the summary API is the pod creation time.
		StartTime: metav1.NewTime(time.Unix(0, podSandbox.CreatedAt)),
		Network:   cadvisorInfoToNetworkStats(caPodSandbox.Name, caPodSandbox),
	}
	podUID := types.UID(s.PodRef.UID)
	if vstats, found := p.resourceAnalyzer.GetPodVolumeStats(podUID); found {
		s.VolumeStats = vstats.Volumes
	}
	return s
}

func (p *criStatsProvider) makeContainerStats(
	stats *runtimeapi.ContainerStats,
	container *runtimeapi.Container,
	rootFsInfo *cadvisorapiv2.FsInfo,
	uuidToFsInfo map[runtimeapi.StorageIdentifier]*cadvisorapiv2.FsInfo,
	caPodStats *cadvisorapiv2.ContainerInfo,
) *statsapi.ContainerStats {
	result := &statsapi.ContainerStats{
		Name: stats.Attributes.Metadata.Name,
		// The StartTime in the summary API is the container creation time.
		StartTime: metav1.NewTime(time.Unix(0, container.CreatedAt)),
		// Work around heapster bug. https://github.com/kubernetes/kubernetes/issues/54962
		// TODO(random-liu): Remove this after heapster is updated to newer than 1.5.0-beta.0.
		CPU: &statsapi.CPUStats{
			UsageNanoCores: proto.Uint64(0),
		},
		Memory: &statsapi.MemoryStats{
			RSSBytes: proto.Uint64(0),
		},
		Rootfs: &statsapi.FsStats{},
		Logs: &statsapi.FsStats{
			Time:           metav1.NewTime(rootFsInfo.Timestamp),
			AvailableBytes: &rootFsInfo.Available,
			CapacityBytes:  &rootFsInfo.Capacity,
			InodesFree:     rootFsInfo.InodesFree,
			Inodes:         rootFsInfo.Inodes,
			// UsedBytes and InodesUsed are unavailable from CRI stats.
			//
			// TODO(yguo0905): Get this information from kubelet and
			// populate the two fields here.
		},
	}
	if caPodStats != nil {
		result.UserDefinedMetrics = cadvisorInfoToUserDefinedMetrics(caPodStats)
	}
	if caPodStats.Spec.HasCpu {
		result.CPU = buildCPUStats(cstat)
	}

	if caPodStats.Spec.HasMemory {
		result.Memory = buildMemoryStats(cstat)
		// availableBytes = memory limit (if known) - workingset
		if !isMemoryUnlimited(caPodStats.Spec.Memory.Limit) {
			availableBytes := caPodStats.Spec.Memory.Limit - cstat.Memory.WorkingSet
			result.Memory.AvailableBytes = &availableBytes
		}
	}
	if stats.WritableLayer != nil {
		result.Rootfs.Time = metav1.NewTime(time.Unix(0, stats.WritableLayer.Timestamp))
		if stats.WritableLayer.UsedBytes != nil {
			result.Rootfs.UsedBytes = &stats.WritableLayer.UsedBytes.Value
		}
		if stats.WritableLayer.InodesUsed != nil {
			result.Rootfs.InodesUsed = &stats.WritableLayer.InodesUsed.Value
		}
	}
	storageID := stats.GetWritableLayer().GetStorageId()
	if storageID != nil {
		imageFsInfo, found := uuidToFsInfo[*storageID]
		if !found {
			imageFsInfo = p.getFsInfo(storageID)
			uuidToFsInfo[*storageID] = imageFsInfo
		}
		if imageFsInfo != nil {
			// The image filesystem UUID is unknown to the local node or there's an
			// error on retrieving the stats. In these cases, we omit those stats
			// and return the best-effort partial result. See
			// https://github.com/kubernetes/heapster/issues/1793.
			result.Rootfs.AvailableBytes = &imageFsInfo.Available
			result.Rootfs.CapacityBytes = &imageFsInfo.Capacity
			result.Rootfs.InodesFree = imageFsInfo.InodesFree
			result.Rootfs.Inodes = imageFsInfo.Inodes
		}
	}

	return result
}

func getCRICadvisorStats(ca cadvisor.Interface) (map[string]cadvisorapiv2.ContainerInfo, error) {
	stats := make(map[string]cadvisorapiv2.ContainerInfo)
	infos, err := getCadvisorContainerInfo(ca)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch cadvisor stats: %v", err)
	}

	for key, info := range infos {
		// On systemd using devicemapper each mount into the container has an
		// associated cgroup. We ignore them to ensure we do not get duplicate
		// entries in our summary. For details on .mount units:
		// http://man7.org/linux/man-pages/man5/systemd.mount.5.html
		if strings.HasSuffix(key, ".mount") {
			continue
		}
		// Build the Pod key if this container is managed by a Pod
		if !isPodManagedContainer(&info) {
			continue
		}
		stats[path.Base(key)] = info
	}
	return stats, nil
}
