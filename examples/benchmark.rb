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

  def init
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

  def init
    @sum = 0
  end

  def process(count)
    @sum += count
  end

  def done
    puts @sum
  end

end

range = RangeEmitter.new concurrency: 4, router: :shuffle
repeater = Repeater.new concurrency: 4
counter = Counter.new concurrency: 4
summer = Summer.new

topology = Flurry::Topology.new
topology.add_processor(:range,    range)
topology.add_processor(:repeater, repeater)
topology.add_processor(:counter,  counter)
topology.add_processor(:summer,   summer)

topology.add_link(:range, :repeater)
topology.add_link(:repeater, :counter)
topology.add_link(:counter, :summer)

topology.begin_computation
topology.emit(:range, [0, 1_000_000])
topology.emit(:range, [0, 1_000_000])
topology.emit(:range, [0, 1_000_000])
topology.emit(:range, [0, 1_000_000])
topology.end_computation

stats = Flurry::Stats.aggregate(topology)
Flurry::Stats.pretty_print(stats)
