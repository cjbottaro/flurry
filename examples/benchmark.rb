require "flurry"

class RangeEmitter
  include Flurry::Processor

  def process(message)
    lower, upper = message
    (lower...upper).each{ |i| emit(i) }
  end

end

class Repeater
  include Flurry::Processor

  def process(object)
    emit(object)
  end
end

class Counter
  include Flurry::Processor

  def initial_state
    @count = 0
  end

  def process(object)
    @count += 1
  end

  def done
    emit(@count)
  end

end

class Summer
  include Flurry::Processor

  def initial_state
    @sum = 0
  end

  def process(count)
    @sum += count
  end

  def done
    puts @sum
  end

end

topology = Flurry::Topology.new
topology.add_processor(:range,    RangeEmitter, concurrency: 4, router: Flurry::Router::Shuffle)
topology.add_processor(:repeater, Repeater, concurrency: 4)
topology.add_processor(:counter,  Counter, concurrency: 4)
topology.add_processor(:summer,   Summer)

topology.add_link(:range, :repeater)
topology.add_link(:repeater, :counter)
topology.add_link(:counter, :summer)

topology.begin_computation
topology.emit(:range, [0, 1_000])
topology.emit(:range, [0, 1_000])
topology.emit(:range, [0, 1_000])
topology.emit(:range, [0, 1_000])
topology.end_computation

Flurry::Stats.pretty_print(topology.stats)
