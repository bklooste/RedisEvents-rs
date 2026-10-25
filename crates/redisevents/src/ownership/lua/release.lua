local removed = 0
for i = 2, #ARGV do
  if redis.call('HGET', KEYS[1], ARGV[i]) == ARGV[1] then
    redis.call('HDEL', KEYS[1], ARGV[i])
    removed = removed + 1
  end
end
return removed