;;defines a simple entry point for establishing
;;marathon peers over hazelcast using the
;;api from hazeldemo.
(ns m4peer.core
  (:require [chazel.core :as ch]
            [marathon.analysis.random :as random]
            [marathon.analysis :as a]
            [hazeldemo.client :as client]
            [hazeldemo.worker :as w]
            [hazeldemo.core :as core]
            [chazel.core :as ch]
            [m4peer.patch]
            [clojure.core.async :as async :refer
             [>! <! >!! <!! put! take! chan]]))

(def +workers+ (marathon.analysis.util/guess-physical-cores))
;;need a worker.
(def work-pool
  (do (when (seq @w/workers)
        (println [:reloading-peer])
        (w/kill-all!))
      (w/spawn-workers! +workers+)))

;;from random runs examples
(comment
  ;way to invoke functions
  (def path "~/repos/notional/supplyvariation-testdata.xlsx")
  (def proj (a/load-project path))
  (def phases [["comp" 1 821] ["phase-1" 822 967]])

  (def run2
    (binding [random/*noisy* 1.0]
      (random/run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 5
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths random/default-compo-lengths
        :levels 3)))

  (def run3
    (binding [random/*noisy* 1.0
              random/*run-site* :cluster]
      (random/run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 5
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths random/default-compo-lengths
        :levels 3)))

 

  ;;We can just use run....and pass it high level args to control behavior.
  ;;This run will run 1 rep, degrading supply by 1, from a range of 1.5*initial AC supply,
  ;;to 0 AC, with the default component lengths to distribute random cycles by, using
  ;;a predefined set of phases to report results.
  (def run1
    (binding [*noisy* 1.0]
      (run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 1
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths default-compo-lengths)))

  ;;This run will run 1 rep, degrading supply by 1, from a range of 1.5*initial AC supply,
  ;;to 0 AC, with the default component lengths to distribute random cycles by, using
  ;;a predefined set of phases to report results.  It will only perform 3 levels of
  ;;supply experiments, distributing the levels evenly across the range [lower*supply upper*supply].
  ;;It will perform 5 replications at each level.  All messages will be logged and
  ;;printed to output cleanly as well.
  (def run2
    (binding [*noisy* 1.0]
      (run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 5
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths default-compo-lengths
        :levels 3)))

  ;;this will generate a different root seed every time, as opposed
  ;;to the previous 2 examples, which will always use +default-seed+
  ;;or 42, unless supplied.
  (def random-run-ex
    (binding [*noisy* 1.0]
      (random-run "~/repos/notional/supplyvariation-testdata.xlsx"
        :reps 5
        :phases phases
        :lower 0 :upper 1.5
        :compo-lengths default-compo-lengths
        :levels 3)))

  ;;This will change out the default ac supply reduction experiments.
  ;;We now leverage at most a 65-level NOLH design varying all
  ;;components.  At best, we do a full-factorial design of < 65.
  (def random-run-nolh
    (binding [*noisy* 1.0
              *project->experiments* project->nolh-experiments]
      (random-run "~/repos/notional/supplyvariation-testdata.xlsx"
                  :reps 1
                  :phases phases
                  :lower 0 :upper 1.5
                  :compo-lengths default-compo-lengths)))

  (def random-run-exhaustive
    (binding [*noisy* 1.0
              *project->experiments* project->full-factorial-experiments]
      (random-run "~/repos/notional/supplyvariation-testdata.xlsx"
                  :reps 1
                  :phases phases
                  :lower 0 :upper 1.5
                  :compo-lengths default-compo-lengths)))

  ;;some defaults...
  (def phases [["comp-1"  1   730]
               ["phase-1" 731 763]
               ["phase-2" 764 883]
               ["phase-3" 884 931]
               ["phase-4" 932 1699]
               ["comp-2"  1700 2430]])

