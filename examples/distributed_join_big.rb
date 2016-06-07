require "flurry"

class JoiningRouter < Flurry::Router::Group
  def routing_key(record)
    case record[0]
    when "User"
      record[1]
    when "Post"
      record[2]
    else
      raise ArgumentError, "expect User or Post, got #{record.inspect}"
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
    when "User"
      process_user(record)
    when "Post"
      process_post(record)
    end
  end

  def process_user(user)
    type, id, name = user
    @left[id] ||= []
    @left[id] << user
  end

  def process_post(post)
    type, id, user_id, title = post
    @right[user_id] ||= []
    @right[user_id] << post
  end

  def done
    @left.each do |person_id, people|
      vehicles = @right[person_id]
      if vehicles
        people.each do |person|
          vehicles.each do |vehicle|
            joined = person + vehicle
            #print "#{joined.inspect}\n"
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
topology.emit :reader, "../data_gen/users_small.csv"
topology.emit :reader, "../data_gen/posts_small.csv"
topology.end_computation
