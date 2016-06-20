require "flurry/router"

module Flurry
  class Topology

    DEFAULT_COMPONENT_OPTIONS = {
      concurrency: 1,
      router: Router::Random
    }

    attr_reader :components, :links, :stats
    private :components, :links

    def initialize
      @components = {}
      @links = []
    end

    def add_processor(name, processor, options = {})
      options = DEFAULT_COMPONENT_OPTIONS.merge(options)

      component_options, processor_options = options.partition do |option|
        DEFAULT_COMPONENT_OPTIONS.has_key?(option)
      end

      processor = case processor
      when Flurry::Processor
        processor
      when Class
        processor.new(processor_options)
      else
        raise ArgumentError, "unexpected processor: #{processor.inspect}"
      end

      router = case options[:router]
      when Flurry::Router
        router
      when Class
        options[:router].new
      when Array
        router_klass, *router_options = options[:router]
        router_klass.new(*router_options)
      else
        raise ArgumentError, "unexpected router options: #{options[:router]}"
      end

      concurrency = options[:concurrency]

      @components[name] = Component.new(processor, concurrency, router)

      self
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

    def begin_computation(*args, &block)
      @stats = nil
      components.each do |name, component|
        routers       = outgoing_routers(name)
        incoming_pids = incoming_pids(name)

        component.setup(routers, incoming_pids, args)
      end

      block.call(self) if block_given?
      end_computation
    ensure
      # Kill the processes.
      components.values.each(&:terminate)
    end

    def emit(name, message)
      components[name].emit(message)
    end

    def end_computation
      # Block until done.
      components.each do |name, component|
        if incoming_links(name) == []
          component.end_computation
        end
      end

      # Collect the stats.
      @stats = components.inject({}) do |memo, (name, component)|
        memo[name] = component.worker_stats
        memo
      end
    end

  private

    def outgoing_routers(name)
      outgoing_links(name).map{ |name| components[name].router }
    end

    def incoming_pids(name)
      incoming_links(name).map{ |name| components[name].pids }.flatten
    end

  end
end
