require "flurry"

class JoiningRouter < Flurry::Router::Group
  def routing_key(record)
    case record[0]
    when "Person"
      record[1]
    when "Vehicle"
      record[2]
    end
  end
end

class EqJoiner
  include Flurry::Processor

  def init
    @left = {}
    @right = {}
  end

  def process(record)
    case record[0]
    when "Person"
      process_person(record)
    when "Vehicle"
      process_vehicle(record)
    end
  end

  def process_person(person)
    type, id, name = person
    @left[id] ||= []
    @left[id] << person
  end

  def process_vehicle(vehicle)
    type, id, person_id, name = vehicle
    @right[person_id] ||= []
    @right[person_id] << vehicle
  end

  def done
    @left.each do |person_id, people|
      vehicles = @right[person_id]
      if vehicles
        people.each do |person|
          vehicles.each do |vehicle|
            joined = person + vehicle
            print "#{joined.inspect}\n"
          end
        end
      end
    end
  end

end

class FileReader
  include Flurry::Processor
  def process(file_name)
    File.open(file_name, "r") do |f|
      f.each_line do |line|
        emit(line.chomp.split(","))
      end
    end
  end
end

topology = Flurry::Topology.new

topology.add_processor :reader, FileReader, concurrency: 2, router: :shuffle
topology.add_processor :joiner, EqJoiner, concurrency: 4, router: JoiningRouter.new

topology.add_link :reader, :joiner

topology.begin_computation
topology.emit :reader, "../tempest/examples/distributed_join/people.csv"
topology.emit :reader, "../tempest/examples/distributed_join/vehicles.csv"
topology.end_computation
