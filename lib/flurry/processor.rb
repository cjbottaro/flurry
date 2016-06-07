module Flurry
  module Processor

    DEFAULT_ATTRIBUTES = {
      concurrency: 1,
      router: :random
    }

    attr_reader :workers, :router
    attr_accessor :outgoing_routers, :stats

    def initialize(attributes = {})
      attributes = DEFAULT_ATTRIBUTES.merge(attributes)
      attributes.each do |k, v|
        instance_variable_set("@#{k}", v)
      end

      init

      @workers = @concurrency.times.map{ Worker.new(self) }

      case attributes[:router]
      when :random
        @router = Router::Random.new
      when :shuffle
        @router = Router::Shuffle.new
      when :group
        @router = Router::Group.new
      when Router
        @router = attributes[:router]
      else
        raise "unexpected router: ##{router.inspect}"
      end

      @router.set_workers(@workers)

    end

    def init
    end

    def done
    end

    def emit(message)
      outgoing_routers.each{ |router| router.route(message) }
      stats.emit_count += 1
    end

  end
end
