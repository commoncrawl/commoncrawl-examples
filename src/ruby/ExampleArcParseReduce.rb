#! /usr/bin/env ruby

curr = nil
sum  = 0

ARGF.each do |line|

  # split key and value on tab character
  key = line.chomp

  # if the current key hasn't been set yet, set it
  if !curr

    curr = key
    sum  = 0

  # if a new key is found, emit the current key ...
  elsif key != curr && sum > 0

    STDOUT.puts(curr + "\t" + sum.to_s())

    # ... then set up a new key
    curr = key
    sum  = 0

  end

  # add to count for this current key
  sum += 1

end
