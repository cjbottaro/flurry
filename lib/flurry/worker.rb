require "gen_server"

module Flurry
  class Worker
    include GenServer

    def init(processor)
      @processor = processor
    end

    def handle_call(from, message)
      type, message = message
      case type
      when :message
        handle_call_message(message)
      when :setup
        handle_call_setup(message)
      when :done
        handle_call_done(from)
      else
        raise ArgumentError, "unexpected message type: #{type.inspect}"
      end
    end

    def handle_call_setup(hash)
      # puts hash.inspect
      @processor.outgoing_routers = hash[:routers]
      @incoming_pids = hash[:incoming_pids]
      @done_from = []
    end

    def handle_call_done(from)
      #puts "<#{Process.pid}> Got done from #{from}"
      @done_from << from
      if @incoming_pids == [] || Set.new(@done_from) == Set.new(@incoming_pids)
        #puts "I am done: #{@done_from.inspect}"
        @processor.done
        @processor.outgoing_routers.each{ |router| router.broadcast([:done]) }
      end
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
      @processor.process(message)
    end

    def emit(message)
      cast [:message, message]
    end

  private

  end
end
