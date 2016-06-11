module Flurry
  class Stats

    attr_accessor :message_count,
                  :emit_count,
                  :start_at,
                  :first_message_at,
                  :done_received_at,
                  :done_at,
                  :code_time,
                  :idle_time

    def self.aggregate(topology)
      topology.components.inject({}) do |memo, (name, components)|
        memo[name] = components.workers.inject({}) do |memo, worker|
          memo[worker.inspect] = worker.call [:stats]
          memo
        end
        memo
      end
    end

    def self.pretty_print(aggregation)
      aggregation.each do |name, stats_by_pid|
        puts name
        stats_by_pid.each do |pid, stats|
          puts "  #{pid}"
          puts "    messages          : %d" % stats.message_count
          puts "    emits             : %d" % stats.emit_count
          puts "    real time         : %.5f" % stats.real_time
          puts "    wait time         : %.5f" % stats.wait_time
          puts "    code time         : %.5f" % stats.code_time
          puts "    idle time         : %.5f" % stats.idle_time
          puts "    done time         : %.5f" % stats.done_time
          puts "    real throughput   : %.5f" % stats.real_throughput
          puts "    code throughput   : %.5f" % stats.code_throughput
          puts "    emit throughput   : %.5f" % stats.emit_throughput
        end
      end
    end

    def initialize
      self.message_count = 0
      self.emit_count = 0
      self.code_time = 0
      self.idle_time = 0
    end

    def add_time(field)
      t1 = Time.now
      result = yield
      send("#{field}=", send(field) + (Time.now - t1))
      result
    end

    def real_time
      done_received_at - start_at
    end

    def done_time
      done_at - done_received_at
    end

    def wait_time
      if first_message_at
        first_message_at - start_at
      else
        0
      end
    end

    def real_throughput
      message_count / real_time
    end

    def code_throughput
      message_count / code_time
    end

    def emit_throughput
      emit_count / real_time
    end

  end
end
