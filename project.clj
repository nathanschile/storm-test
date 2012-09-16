(defproject storm-test "0.2.0"
  :description "Testing utilities for storm"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :dependencies [[org.clojure/clojure "1.4.0"]
                 []]
  :aot [storm.test.visualization]
  :dev-dependencies [[storm "0.8.1"]])
