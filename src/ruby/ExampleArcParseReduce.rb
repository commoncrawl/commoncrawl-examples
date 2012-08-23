#! /usr/bin/env ruby

curr = nil
sum  = 0

ARGF.each do |line|

  # the entire line is the key
  key = line.chomp

  # if the current key hasn't been set yet, set it
  if !curr

    curr = key
    sum  = 0

  # if a new key is found, emit the current key ...
  elsif key != curr && sum > 0

    if sum > 2 
      STDOUT.puts(curr + "\t" + sum.to_s())
    end

    # ... then set up a new key
    curr = key
    sum  = 0

  end

  # add to count for this current key
  sum += 1

end
