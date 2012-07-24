#!/usr/bin/env ruby

require 'rubygems'
require 'open3'

# Inline this so we don't have to copy a file while bootstrapping
class ArcFile

  include Enumerable

  def initialize( input_stream )
    @handle=input_stream
  end

  def each
    return self.to_enum unless block_given?
    begin
      # See http://www.archive.org/web/researcher/ArcFileFormat.php
      # for information about the ARC format once it is decompressed
      main_header=@handle.readline.strip
      main_header_body=@handle.read(Integer(main_header.split.last))

      loop do
        begin
          record_header=@handle.readline.strip
          size=Integer(record_header.split.last)
          record_body=@handle.read(size)
          unless (byte=@handle.read(1))=="\n"
            raise ArgumentError, "#{self.class}: Corrupt ARCfile? Expected \\n as record terminator, got '#{byte}'"
          end
          yield [record_header, record_body]
        rescue EOFError
          break nil
        end
      end
    rescue
      raise "#{self.class}: Error processing - #{$!}"
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
  arcfile.each {|header, body|

    if header
      url = header.split[0]
      mimetype = header.split[3]

      next if !mimetype || mimetype.chomp() == ""

      next if !url || url.chomp() == ""

      fileext = File.extname(url.split(/[\?&%;]/).first())

      next if !fileext || fileext.chomp() == ""
      
      STDOUT.puts(mimetype.downcase() + "\t" + fileext.downcase())

    end
  }
}

