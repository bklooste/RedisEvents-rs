local me = ARGV[1]
local ttl = tonumber(ARGV[2])
local n = tonumber(ARGV[3])
local presence = ARGV[4]

redis.call('HSET', KEYS[1], presence, me)
redis.call('HEXPIRE', KEYS[1], ttl, 'FIELDS', 1, presence)

local members = 0
local names = redis.call('HKEYS', KEYS[1])
for i = 1, #names do
  if string.sub(names[i], 1, 2) == 'i:' then members = members + 1 end
end
if members < 1 then members = 1 end
local cap = math.ceil(n / members)

local held = {}
for p = 0, n - 1 do
  if redis.call('HGET', KEYS[1], tostring(p)) == me then held[#held + 1] = p end
end

while #held > cap do
  redis.call('HDEL', KEYS[1], tostring(table.remove(held)))
end

for i = 1, #held do
  redis.call('HEXPIRE', KEYS[1], ttl, 'FIELDS', 1, tostring(held[i]))
end

if #held < cap then
  for p = 0, n - 1 do
    if #held >= cap then break end
    local f = tostring(p)
    if redis.call('HSETNX', KEYS[1], f, me) == 1 then
      redis.call('HEXPIRE', KEYS[1], ttl, 'FIELDS', 1, f)
      held[#held + 1] = p
    end
  end
end

table.sort(held)
return held