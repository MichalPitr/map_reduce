package master

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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
	clientset := createKubernetesClient()
	numNodes := getNumberOfNodes(clientset)
	mustValidateConfig(cfg, numNodes)

	jobId := fmt.Sprintf("job-%s", time.Now().Format("2006-01-02-15-04-05"))
	log.Printf("Running master: %s", jobId)

	mustCreateJobDir(cfg.NfsPath, jobId)
	fileRanges := partitionInputFiles(cfg.InputDir, cfg.NumMappers)

	t0 := time.Now()
	launchMappers(cfg, clientset, jobId, fileRanges)
	waitForJobsToComplete(clientset, jobId, "mapper")
	log.Printf("Mappers took %v to finish", time.Since(t0))

	t1 := time.Now()
	launchReducers(cfg, clientset, jobId)
	waitForJobsToComplete(clientset, jobId, "reducer")
	log.Printf("Reducers took %v to finish", time.Since(t1))
	log.Printf("Total runtime: %v", time.Since(t0))
}

func mustCreateJobDir(path string, jobId string) {
	jobDir := filepath.Join(path, jobId)
	err := os.Mkdir(jobDir, 0777)
	if err != nil {
		log.Fatalf("Error creating job directory: %v", err)
	}
}

func mustValidateConfig(cfg *config.Config, numNodes int) {
	if numNodes == 0 {
		log.Fatal("Need at least 1 node in the cluster.")
	} else if numNodes < cfg.NumMappers || numNodes < cfg.NumReducers {
		log.Fatal("More mappers or reducers than available nodes.")
	}

	if cfg.Image == "" {
		log.Fatal("Must provide image.")
	}
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

func launchMappers(cfg *config.Config, clientset *kubernetes.Clientset, jobId string, fileRanges []string) {
	for i := 0; i < cfg.NumMappers; i++ {
		mapperId := fmt.Sprintf("mapper-%d", i)
		_ = clientset
		log.Printf("Creating %s for %s", mapperId, fileRanges[i])
		job := createMapperJobSpec(cfg, jobId, mapperId, fileRanges[i])
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			log.Printf("Failed to create a job: %v", err)
			os.Exit(1)
		}
	}
}

func waitForJobsToComplete(clientset *kubernetes.Clientset, jobName, suffix string) {
	labelSelector := fmt.Sprintf("job-group=%s-%s", jobName, suffix)
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

func createMapperJobSpec(cfg *config.Config, jobId, mapperId, fileRange string) *batchv1.Job {
	outputDir := filepath.Join(cfg.NfsPath, jobId, mapperId)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      mapperId,
			Namespace: "default",
			Labels: map[string]string{
				"job-group": jobId + "-mapper",
			},
		},
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    "worker",
							Image:   cfg.Image,
							Command: []string{"./mapreduce", "--mode", "mapper", "--input-dir", cfg.InputDir, "--output-dir", outputDir, "--file-range", fileRange},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "nfs-storage",
									MountPath: cfg.NfsPath,
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

func launchReducers(cfg *config.Config, clientset *kubernetes.Clientset, jobId string) {
	for i := 0; i < cfg.NumReducers; i++ {
		log.Printf("Creating reducer-%d", i)
		job := createReducerJobSpec(cfg, jobId, i)
		_, err := clientset.BatchV1().Jobs("default").Create(context.TODO(), job, metav1.CreateOptions{})
		if err != nil {
			log.Fatalf("Failed to create a job: %v", err)
		}
	}
}

func createReducerJobSpec(cfg *config.Config, jobId string, reducerId int) *batchv1.Job {
	reducerName := fmt.Sprintf("reducer-%d", reducerId)
	inputDir := filepath.Join(cfg.NfsPath, jobId)
	outputDir := filepath.Join(cfg.NfsPath, jobId)
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      reducerName,
			Namespace: "default",
			Labels: map[string]string{
				"job-group": jobId + "-reducer",
			},
		},
		Spec: batchv1.JobSpec{
			Template: v1.PodTemplateSpec{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name:    "worker",
							Image:   cfg.Image,
							Command: []string{"./mapreduce", "--mode", "reducer", "--input-dir", inputDir, "--output-dir", outputDir, "--reducer-id", strconv.Itoa(reducerId)},
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "nfs-storage",
									MountPath: cfg.NfsPath,
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
