#!/usr/bin/env ruby

require 'rubygems'
require 'open3'
require 'uri'

# Inline these classes so we don't have to copy a file while bootstrapping
class ArcRecord
  attr_accessor :num, :url, :ip_address, :archive_date, :content_type, :content_length, :content
end

class ArcFile

  include Enumerable

  def initialize(input_stream)
    @handle=input_stream
  end

  def each
    return self.to_enum unless block_given?
    begin
      # See http://www.archive.org/web/researcher/ArcFileFormat.php
      # for information about the ARC format once it is decompressed
      file_header = @handle.readline.strip
      @handle.read(Integer(file_header.split.last))
      i=1

      loop do
        begin
          fields = @handle.readline.strip.split(" ")
          raise "Invalid ARC record header found"       if fields.length != 5
          warn("Invalid protocol in ARC record header") if not fields[0].to_s.start_with?("http://", "https://")

          record = ArcRecord.new
          record.num            = i
          record.url            = fields[0].to_s
          record.ip_address     = fields[1].to_s
          record.archive_date   = fields[2].to_s
          record.content_type   = fields[3].to_s
          record.content_length = Integer(fields[4])
          record.content = @handle.read(record.content_length)
          i = i+1

          yield record

        rescue EOFError
          break nil
        end
      end
    #rescue
    #  raise "#{self.class}: Record ##{i} - Error - #{$!}"
    end
  end

end

CHUNKSIZE=1024*1024

# All warnings will end up in the EMR stderr logs.
warn("Starting up GZIP process, piping #{CHUNKSIZE/1024}KB chunks at a time")

# Ruby GzipReader is unable to unzip these files, but unix gunzip can
# Also means we don't need to eat much RAM, because everything is streaming.
Open3.popen3('gunzip -c') {|sin,sout,serr,thr|

  # Create an ArcFile instance which will receive gunzip's stdout
  arcfile = ArcFile.new(sout)

  Thread.new do
    loop do
      begin
        chunk = STDIN.readpartial(CHUNKSIZE)
        sin.write(chunk)
        Thread.pass()
      rescue EOFError
        warn("End of input, flushing and closing stream to GZIP")
        sin.close() # which will send an EOF to the ArcFile
        break nil
      end
    end
  end

  # Now we have a lazy ArcFile that we can treat as an Enumerable.
  arcfile.each {|record|
    if record
      begin
        # work around Ruby URI library's lack of support for URLs with underscore
        uri = URI.parse(record.url.delete("_"))
        STDOUT.puts(uri.host.downcase())
      rescue URI::InvalidURIError
        warn("ARC file contains invalid URL: "+record.url)
        next
      end
    end
  }
}

