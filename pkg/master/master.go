package master

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/MichalPitr/map_reduce/pkg/config"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

func Run(cfg *config.Config) {
	log.Printf("Running master...")
	clientset := createKubernetesClient()
	numNodes := getNumberOfNodes(clientset)
	if numNodes == 0 {
		log.Printf("Need at least 1 node in the cluster.")
		os.Exit(1)
	}
	jobName := fmt.Sprintf("job-%s", time.Now().Format("2006-01-02-15-04-05"))

	// Host nfs mount.
	err := os.Mkdir("/mnt/"+jobName, 0777)
	if err != nil {
		log.Printf("Error creating job folder: %v", err)
		os.Exit(1)
	}

	fileRanges := partitionInputFiles(cfg.InputDir, numNodes)
	fmt.Println(fileRanges)

	startTime := time.Now()
	lunchJobs(clientset, jobName, numNodes, fileRanges)
	waitForJobsToComplete(clientset, jobName)
	log.Printf("Mappers took %v to finish", time.Since(startTime))
}

func partitionInputFiles(inputDir string, partitions int) []string {
	entries, err := os.ReadDir(inputDir)
	if err != nil {
		log.Printf("Failed to read contents of %s, error: %v", inputDir, err)
		os.Exit(1)
	}
	files := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		files = append(files, entry.Name())
	}

	slices.Sort(files)
	fileRanges := make([]string, partitions)
	filesInPartition := len(files) / partitions

	extra := len(files) % partitions
	currentStart := 0
	for i := 0; i < partitions; i++ {
		currentEnd := currentStart + filesInPartition - 1
		if extra > 0 {
			currentEnd++
			extra--
		}
		first := strings.Split(files[currentStart], "-")
		last := strings.Split(files[currentEnd], "-")
		fileRange := first[0] + "-" + first[1] + "-" + last[1]
		fileRanges[i] = fileRange

		currentStart = currentEnd + 1
	}

	return fileRanges
}

func createKubernetesClient() *kubernetes.Clientset {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	flag.Parse()

	// Use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// Create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	return clientset
}

func getNumberOfNodes(clientset *kubernetes.Clientset) int {
	nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	return len(nodes.Items)
}

func lunchJobs(clientset *kubernetes.Clientset, jobName string, numJobs int, fileRanges []string) {
	for i := 0; i < numJobs; i++ {
		jobId := fmt.Sprintf("%s-job-%d", jobName, i+1)
		_ = clientset
		// job := createJobSpec(jobName, jobId)
		fmt.Printf("Creating mapper %s for %s\n", jobId, fileRanges[i])
		job := createMapperJobSpec(jobName, jobId, fileRanges[i])
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			log.Printf("Failed to create a job: %v", err)
			os.Exit(1)
		}
	}
}

func waitForJobsToComplete(clientset *kubernetes.Clientset, jobName string) {
	labelSelector := fmt.Sprintf("job-group=%s", jobName)
	for {
		jobs, err := clientset.BatchV1().Jobs("default").List(context.TODO(), metav1.ListOptions{
			LabelSelector: labelSelector,
		})
		if err != nil {
			log.Printf("Failed to list jobs: %v", err)
			os.Exit(1)
		}

		allCompleted := true
		for _, job := range jobs.Items {
			if job.Status.Succeeded == 0 {
				allCompleted = false
				break
			}
		}

		if allCompleted {
			log.Println("All jobs completed.")
			break
		}

		log.Println("Waiting for jobs to finish.")
		time.Sleep(10 * time.Second)
	}
}

func createMapperJobSpec(jobName, jobId, fileRange string) *batchv1.Job {
	nfsBaseDir := "/mnt/nfs/"
	inputDir := nfsBaseDir + "input/"
	outputDir := nfsBaseDir + jobName + "/" + jobId + "/"
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobId,
			Namespace: "default",
			Labels: map[string]string{
				"job-group": jobName,
			},
		},
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    "worker",
							Image:   "michalpitr/mapreduce:latest",
							Command: []string{"./mapreduce", "--mode", "mapper", "--input-dir", inputDir, "--output-dir", outputDir, "--file-range", fileRange},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "nfs-storage",
									MountPath: "/mnt/nfs",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "nfs-storage",
							VolumeSource: v1.VolumeSource{
								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
									ClaimName: "nfs-pvc",
								},
							},
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
				},
			},
		},
	}
}

func createJobSpec(jobName, jobId string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobId,
			Namespace: "default",
			Labels: map[string]string{
				"job-group": jobName,
			},
		},
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    "worker",
							Image:   "alpine",
							Command: []string{"/bin/sh", "-c"},
							Args: []string{
								"mkdir -p /mnt/nfs/" + jobName + "/" + jobId + ";" +
									"sleep 60;" +
									"echo 'hello world' > /mnt/nfs/" + jobName + "/" + jobId + "/result1.txt",
							},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "nfs-storage",
									MountPath: "/mnt/nfs",
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "nfs-storage",
							VolumeSource: v1.VolumeSource{
								PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
									ClaimName: "nfs-pvc",
								},
							},
						},
					},
					RestartPolicy: v1.RestartPolicyNever,
				},
			},
		},
	}
}
