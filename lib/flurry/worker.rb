require "gen_server"
require "flurry/stats"

module Flurry
  class Worker
    include GenServer

    def init(processor)
      @processor = processor
    end

    def handle_call(from, message)
      case message.shift
      when :message
        handle_call_message(*message)
      when :setup
        handle_call_setup(*message)
      when :done
        handle_call_done(from)
      when :stats
        handle_call_stats
      else
        raise ArgumentError, "unexpected message type: #{type.inspect}"
      end
    end

    def handle_call_setup(routers, incoming_pids, args)
      @routers = routers
      @incoming_pids = incoming_pids

      @processor.set_worker(self)
      @processor.initial_state
      @processor.before_begin_computation(*args)

      @done_from = []
      @stats = Flurry::Stats.new
      @stats.start_at = Time.now

      Process.setproctitle("FlurryWorker - #{@processor.class}")
    end

    def handle_call_done(from)
      @done_from << from
      if @incoming_pids == [] || Set.new(@done_from) == Set.new(@incoming_pids)
        @stats.done_received_at = Time.now
        @processor.done
        @stats.done_at = Time.now
        @routers.each{ |router| router.broadcast([:done]) }
      end
    end

    def handle_call_stats
      @stats
    end

    def handle_cast(message)
      type, message = message
      case type
      when :message
        handle_cast_message(message)
      else
        raise ArgumentError, "unexpected message type: #{type.inspect}"
      end
    end

    def handle_cast_message(message)
      if @stats.first_message_at
        @stats.idle_time += Time.now - @last_message_processed_at
      else
        @stats.first_message_at = Time.now
        @stats.idle_time += @stats.first_message_at - @stats.start_at
      end
      @stats.add_time(:code_time){ @processor.process(message) }
      @stats.message_count += 1
      @last_message_processed_at = Time.now
    end

    def emit(message)
      cast [:message, message]
    end

    def emit_from_processor(message)
      @routers.each{ |router| router.route(message) }
      @stats.emit_count += 1
    end

  private

  end
end
