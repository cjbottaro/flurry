module Flurry
  class Topology

    attr_reader :processors

    def initialize
      @processors = {}
      @links = []
    end

    def add_processor(name, processor, options = {})
      case processor
      when Flurry::Processor
        @processors[name] = processor
      when Class
        @processors[name] = processor.new(options)
      else
        raise ArgumentError, "unexpected processor: #{processor.inspect}"
      end
    end

    def add_link(src, dst)
      @links << [src, dst]
    end

    def outgoing_links(name)
      @links.inject([]) do |memo, (src, dst)|
        memo << dst if src == name
        memo
      end
    end

    def incoming_links(name)
      @links.inject([]) do |memo, (src, dst)|
        memo << src if dst == name
        memo
      end
    end

    def begin_computation
      processors.each do |name, processor|
        processor.workers.each do |worker|
          setup = {
            routers: outgoing_routers(name),
            incoming_pids: incoming_pids(name)
          }
          worker.call [:setup, setup]
        end
      end
    end

    def emit(name, message)
      processors[name].router.route(message)
    end

    def end_computation
      processors.each do |name, processor|
        if incoming_links(name) == []
          processor.workers.each{ |w| w.call [:done] }
        end
      end
    end

  private

    def outgoing_routers(name)
      outgoing_links(name).map{ |name| processors[name].router }
    end

    def incoming_pids(name)
      incoming_links(name).inject([]) do |memo, name|
        processors[name].workers.each do |w|
          memo << w.pid
        end
        memo
      end
    end

  end
end
