package controller

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	netattachdef "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"

	"github.com/kubeovn/kube-ovn/pkg/util"
)

const (
	LbPodImage       = "kubeovn/vpc-nat-gateway:v1.11.0"
	INIT_ROUTE_TABLE = "init"
	POD_EIP_ADD      = "eip-add"
	POD_DNAT_ADD     = "dnat-add"
)

func genLbSvcDpName(name string) string {
	return fmt.Sprintf("lb-svc-%s", name)
}

func (c *Controller) applyExternalNetwork(svc *corev1.Service) error {
	nic := "eth0"
	if svc.Annotations[util.AttachmentNic] != "" {
		nic = svc.Annotations[util.AttachmentNic]
	}

	cfgTmpl := "{\"cniVersion\": \"0.3.0\",\"type\": \"macvlan\",\"master\": \"%NIC%\",\"mode\": \"bridge\"}"
	netCfg := strings.ReplaceAll(cfgTmpl, "%NIC%", nic)

	networkClient := c.config.AttachNetClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(svc.Namespace)
	_, err := networkClient.Get(context.Background(), svc.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			network := &netattachdef.NetworkAttachmentDefinition{
				ObjectMeta: metav1.ObjectMeta{
					Name:      svc.Name,
					Namespace: svc.Namespace,
				},
				Spec: netattachdef.NetworkAttachmentDefinitionSpec{Config: netCfg},
			}
			_, err = networkClient.Create(context.Background(), network, metav1.CreateOptions{})
			if err != nil {
				klog.Errorf("failed to create network attachment definition %s, err: %v", network.Name, err)
				return err
			}
		} else {
			return err
		}
	}

	for {
		network, err := networkClient.Get(context.Background(), svc.Name, metav1.GetOptions{})
		if network != nil && err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	return nil
}

func (c *Controller) genLbSvcDeployment(svc *corev1.Service) (dp *v1.Deployment) {
	replicas := int32(1)
	name := genLbSvcDpName(svc.Name)
	allowPrivilegeEscalation := true
	privileged := true
	labels := map[string]string{
		"app":       name,
		"namespace": svc.Namespace,
	}

	providerName := fmt.Sprintf("%s.%s", svc.Name, svc.Namespace)
	attchSubnetAnnotation := fmt.Sprintf(util.LogicalSwitchAnnotationTemplate, providerName)
	attchIpAnnotation := fmt.Sprintf(util.IpAddressAnnotationTemplate, providerName)
	podAnnotations := map[string]string{
		util.AttachmentNetworkAnnotation: fmt.Sprintf("%s/%s", svc.Namespace, svc.Name),
		attchSubnetAnnotation:            svc.Annotations[attchSubnetAnnotation],
	}
	if svc.Spec.LoadBalancerIP != "" {
		podAnnotations[attchIpAnnotation] = svc.Spec.LoadBalancerIP
	}

	dp = &v1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels:      labels,
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "lb-svc",
							Image:           LbPodImage,
							Command:         []string{"bash"},
							Args:            []string{"-c", "while true; do sleep 10000; done"},
							ImagePullPolicy: corev1.PullIfNotPresent,
							SecurityContext: &corev1.SecurityContext{
								Privileged:               &privileged,
								AllowPrivilegeEscalation: &allowPrivilegeEscalation,
							},
						},
					},
				},
			},
			Strategy: v1.DeploymentStrategy{
				Type: v1.RecreateDeploymentStrategyType,
			},
		},
	}
	return
}

func (c *Controller) createLbSvcPod(svc *corev1.Service) error {
	// check or create deployment
	needToCreate := false
	if _, err := c.config.KubeClient.AppsV1().Deployments(svc.Namespace).Get(context.Background(), genLbSvcDpName(svc.Name), metav1.GetOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			needToCreate = true
		} else {
			return err
		}
	}

	newDp := c.genLbSvcDeployment(svc)

	if needToCreate {
		_, err := c.config.KubeClient.AppsV1().Deployments(svc.Namespace).
			Create(context.Background(), newDp, metav1.CreateOptions{})

		if err != nil {
			klog.Errorf("failed to create deployment %s, err: %v", newDp.Name, err)
			return err
		}
		return nil
	} else {
		_, err := c.config.KubeClient.AppsV1().Deployments(svc.Namespace).
			Update(context.Background(), newDp, metav1.UpdateOptions{})

		if err != nil {
			klog.Errorf("failed to update deployment %s, err: %v", newDp.Name, err)
			return err
		}
	}

	return nil
}

func (c *Controller) getLbSvcPod(svcName, svcNamespace string) (*corev1.Pod, error) {
	sel, _ := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{"app": genLbSvcDpName(svcName), "namespace": svcNamespace},
	})

	pods, err := c.podsLister.Pods(svcNamespace).List(sel)
	if err != nil {
		return nil, err
	} else if len(pods) == 0 {
		time.Sleep(2 * time.Second)
		return nil, fmt.Errorf("pod '%s' not exist", genLbSvcDpName(svcName))
	} else if len(pods) != 1 {
		time.Sleep(2 * time.Second)
		return nil, fmt.Errorf("too many pod")
	} else if pods[0].Status.Phase != "Running" {
		time.Sleep(2 * time.Second)
		return nil, fmt.Errorf("pod is not active now")
	}

	return pods[0], nil
}

func (c *Controller) validateSvc(svc *corev1.Service) error {
	providerName := fmt.Sprintf("%s.%s", svc.Name, svc.Namespace)
	attchSubnetAnnotation := fmt.Sprintf(util.LogicalSwitchAnnotationTemplate, providerName)

	if svc.Annotations[attchSubnetAnnotation] == "" {
		return fmt.Errorf("attachment subnet annotation needed, can not find it for svc %s", svc.Name)
	}

	subnet, err := c.subnetsLister.Get(svc.Annotations[attchSubnetAnnotation])
	if err != nil {
		klog.Errorf("failed to get subnet %v", err)
		return err
	}

	if svc.Spec.LoadBalancerIP != "" {
		if ip := net.ParseIP(svc.Spec.LoadBalancerIP); ip == nil {
			return fmt.Errorf("Invalid loadbalancerIP %s for svc %s", svc.Spec.LoadBalancerIP, svc.Name)
		}

		if !util.CIDRContainIP(subnet.Spec.CIDRBlock, svc.Spec.LoadBalancerIP) {
			return fmt.Errorf("The loadbalancerIP %s is not in the range of subnet %s, cidr %v", svc.Spec.LoadBalancerIP, subnet.Name, subnet.Spec.CIDRBlock)
		}
	}
	return nil
}

func (c *Controller) getPodAttachIP(pod *corev1.Pod, svc *corev1.Service) (string, error) {
	var loadBalancerIP string
	var err error

	providerName := fmt.Sprintf("%s.%s", svc.Name, svc.Namespace)
	attchIpAnnotation := fmt.Sprintf(util.IpAddressAnnotationTemplate, providerName)

	if pod.Annotations[attchIpAnnotation] != "" {
		loadBalancerIP = pod.Annotations[attchIpAnnotation]
	} else {
		err = fmt.Errorf("failed to get attachment ip from pod's annotation")
	}

	return loadBalancerIP, err
}

func (c *Controller) deleteLbSvc(svc *corev1.Service) error {
	if _, err := c.config.KubeClient.AppsV1().Deployments(svc.Namespace).Get(context.Background(), genLbSvcDpName(svc.Name), metav1.GetOptions{}); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		} else {
			return err
		}
	}

	if err := c.config.KubeClient.AppsV1().Deployments(svc.Namespace).Delete(context.Background(), genLbSvcDpName(svc.Name), metav1.DeleteOptions{}); err != nil {
		klog.Errorf("failed to delete deployment %s, err: %v", genLbSvcDpName(svc.Name), err)
		return err
	}

	for {
		_, err := c.getLbSvcPod(svc.Name, svc.Namespace)
		if err != nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	networkClient := c.config.AttachNetClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(svc.Namespace)
	network, err := networkClient.Get(context.Background(), svc.Name, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		} else {
			return err
		}
	}

	if err := networkClient.Delete(context.Background(), network.Name, metav1.DeleteOptions{}); err != nil {
		klog.Errorf("failed to delete attachment crd %s, err: %v", network.Name, err)
		return err
	}

	return nil
}

func (c *Controller) execNatRules(pod *corev1.Pod, operation string, rules []string) error {
	cmd := fmt.Sprintf("bash /kube-ovn/lb-svc.sh %s %s", operation, strings.Join(rules, " "))
	klog.V(3).Infof(cmd)
	stdOutput, errOutput, err := util.ExecuteCommandInContainer(c.config.KubeClient, c.config.KubeRestConfig, pod.Namespace, pod.Name, "lb-svc", []string{"/bin/bash", "-c", cmd}...)

	if err != nil {
		if len(errOutput) > 0 {
			klog.Errorf("failed to ExecuteCommandInContainer, errOutput: %v", errOutput)
		}
		if len(stdOutput) > 0 {
			klog.V(3).Infof("failed to ExecuteCommandInContainer, stdOutput: %v", stdOutput)
		}
		return err
	}

	if len(stdOutput) > 0 {
		klog.V(3).Infof("ExecuteCommandInContainer stdOutput: %v", stdOutput)
	}

	if len(errOutput) > 0 {
		klog.Errorf("failed to ExecuteCommandInContainer errOutput: %v", errOutput)
		return errors.New(errOutput)
	}
	return nil
}

func (c *Controller) updatePodAttachNets(pod *corev1.Pod, svc *corev1.Service) error {
	if err := c.execNatRules(pod, INIT_ROUTE_TABLE, []string{}); err != nil {
		klog.Errorf("failed to init route table, err: %v", err)
		return err
	}

	providerName := fmt.Sprintf("%s.%s", svc.Name, svc.Namespace)
	attchIpAnnotation := fmt.Sprintf(util.IpAddressAnnotationTemplate, providerName)
	attchCidrAnnotation := fmt.Sprintf(util.CidrAnnotationTemplate, providerName)
	attchGatewayAnnotation := fmt.Sprintf(util.GatewayAnnotationTemplate, providerName)

	if pod.Annotations[attchCidrAnnotation] == "" || pod.Annotations[attchGatewayAnnotation] == "" {
		return fmt.Errorf("failed to get attachment network info for pod %s", pod.Name)
	}

	loadBalancerIP := pod.Annotations[attchIpAnnotation]
	ipAddr := util.GetIpAddrWithMask(loadBalancerIP, pod.Annotations[attchCidrAnnotation])

	var addRules []string
	addRules = append(addRules, fmt.Sprintf("%s,%s", ipAddr, pod.Annotations[attchGatewayAnnotation]))
	klog.Infof("add eip rules for lb svc pod, %v", addRules)
	if err := c.execNatRules(pod, POD_EIP_ADD, addRules); err != nil {
		klog.Errorf("failed to add eip for pod, err: %v", err)
		return err
	}

	defaultGateway := pod.Annotations[util.GatewayAnnotation]
	for _, port := range svc.Spec.Ports {
		var protocol string
		switch port.Protocol {
		case corev1.ProtocolTCP:
			protocol = "tcp"
		case corev1.ProtocolUDP:
			protocol = "udp"
		case corev1.ProtocolSCTP:
			protocol = "sctp"
		}

		var rules []string
		rules = append(rules, fmt.Sprintf("%s,%d,%s,%s,%d,%s", loadBalancerIP, port.Port, protocol, svc.Spec.ClusterIP, port.TargetPort.IntVal, defaultGateway))
		klog.Infof("add dnat rules for lb svc pod, %v", rules)
		if err := c.execNatRules(pod, POD_DNAT_ADD, rules); err != nil {
			klog.Errorf("failed to add dnat for pod, err: %v", err)
			return err
		}
	}

	return nil
}

func (c *Controller) runUpdateLbSvcPodWorker() {
	for c.processNextUpdateLbSvcPodWorkItem() {
	}
}

func (c *Controller) processNextUpdateLbSvcPodWorkItem() bool {
	obj, shutdown := c.updateLbSvcPodQueue.Get()
	if shutdown {
		return false
	}

	err := func(obj interface{}) error {
		defer c.updateLbSvcPodQueue.Done(obj)
		var svc *corev1.Service
		var ok bool
		if svc, ok = obj.(*corev1.Service); !ok {
			c.updateLbSvcPodQueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected Service in workqueue but got %#v", obj))
			return nil
		}

		if err := c.handleUpdateLbSvcPod(svc); err != nil {
			c.updateLbSvcPodQueue.AddRateLimited(obj)
			return fmt.Errorf("error syncing '%s': %s, requeuing", svc.Name, err.Error())
		}
		c.updateLbSvcPodQueue.Forget(obj)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}
	return true
}

func (c *Controller) handleUpdateLbSvcPod(svc *corev1.Service) error {
	pod, err := c.getLbSvcPod(svc.Name, svc.Namespace)
	if err != nil {
		// There's no other way to delete update pod process in queue,so record err and return if pod does not exist
		klog.Errorf("failed to get lb svc pod, %v", err)
		return nil
	}

	if err := c.updatePodAttachNets(pod, svc); err != nil {
		klog.Errorf("update service %s/%s attachment network failed: %v", svc.Namespace, svc.Name, err)
		return err
	}
	return nil
}
