require "flurry"

class Reader
  include Flurry::Processor
  def process(file_name)
    File.open(file_name, "r") do |f|
      f.each_line do |line|
        emit(line)
      end
    end
  end
end

class LineProcessor
  include Flurry::Processor
  def process(line)
    line.downcase
        .gsub(/[^'a-z0-9]/, " ")
        .split(" ")
        .map{ |word| word.strip }
        .reject{ |word| word == "" }
        .each{ |word| emit(word) }
  end
end

class WordCounter
  include Flurry::Processor

  def init
    @counts = {}
  end

  def process(word)
    @counts[word] ||= 0
    @counts[word] += 1
  end

  def done
    @counts.each{ |word, count| emit([word, count]) }
  end
end

class Printer
  include Flurry::Processor
  def process(word_and_count)
    word, count = word_and_count
    print "#{word} #{count}\n"
  end
end

topology = Flurry::Topology.new
topology.add_processor :reader, Reader
topology.add_processor :line_processor, LineProcessor, concurrency: 4
topology.add_processor :word_counter, WordCounter, concurrency: 4, router: :group
topology.add_processor :printer, Printer

topology.add_link :reader, :line_processor
topology.add_link :line_processor, :word_counter
topology.add_link :word_counter, :printer

topology.begin_computation
topology.emit :reader, ARGV[0]
topology.end_computation
