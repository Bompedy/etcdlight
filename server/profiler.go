package server

import (
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"syscall"
)

func startProfiling() {
	// Initialize CPU profiling
	cpuProfile, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatalf("could not create CPU profile: %v", err)
	}
	pprof.StartCPUProfile(cpuProfile)

	// Enable detailed profiling for blocking operations and mutex contention
	runtime.SetBlockProfileRate(1)     // Record every blocking event
	runtime.SetMutexProfileFraction(1) // Record every mutex contention

	// Signal handling goroutine
	go func() {
		// Create channel for system signals
		sigChan := make(chan os.Signal, 1)

		// Register for termination signals
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		// Wait for termination signal
		sig := <-sigChan
		log.Printf("Received signal %s, stopping profiler...", sig)

		// Stop CPU profiling and close file
		pprof.StopCPUProfile()
		cpuProfile.Close()
		log.Println("CPU profiling stopped")

		// Write memory heap profile
		memProfileFile, _ := os.Create("mem.prof")
		runtime.GC() // Force garbage collection before memory profiling
		pprof.WriteHeapProfile(memProfileFile)
		memProfileFile.Close()
		log.Println("Memory profile written")

		// Write additional profiling data
		profiles := []string{"goroutine", "threadcreate", "block", "mutex"}
		for _, prof := range profiles {
			f, err := os.Create(prof + ".prof")
			if err != nil {
				log.Printf("Could not create %s profile: %v", prof, err)
				continue
			}
			if err := pprof.Lookup(prof).WriteTo(f, 0); err != nil {
				log.Printf("Error writing %s profile: %v", prof, err)
			}
			f.Close()
			log.Printf("%s profile written", prof)
		}

		// Terminate application
		os.Exit(1)
	}()
}
