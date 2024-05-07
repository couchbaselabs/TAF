package com.couchbase.test.val;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;

import java.util.Base64;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import ai.djl.MalformedModelException;
import ai.djl.huggingface.translator.TextEmbeddingTranslatorFactory;
import ai.djl.inference.Predictor;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;

import com.couchbase.test.docgen.WorkLoadSettings;

public class Vector {
    public String[] colors = { "Abbey", "Absolute Zero", "Acadia", "Acapulco", "Acid Green", "Aero", "Aero Blue", "Affair",
            "African Violet", "Air Force Blue", "Air Superiority Blue", "Akaroa", "Alabama Crimson", "Alabaster",
            "Albescent White", "Algae Green", "Alice Blue", "Alien Armpit", "Alizarin Crimson", "Alloy Orange",
            "Allports", "Almond", "Almond Frost", "Alpine", "Alto", "Aluminium", "Amaranth", "Amaranth Pink",
            "Amaranth Purple", "Amaranth Red", "Amazon", "Amber", "American Rose", "Americano", "Amethyst",
            "Amethyst Smoke", "Amour", "Amulet", "Anakiwa", "Android Green", "Anti Flash White", "Antique Brass",
            "Antique Bronze", "Antique Fuchsia", "Antique Ruby", "Antique White", "Anzac", "Ao", "Apache", "Apple",
            "Apple Blossom", "Apple Green", "Apricot", "Apricot White", "Aqua Deep", "Aqua Forest", "Aqua Haze",
            "Aqua Island", "Aqua Spring", "Aqua Squeeze", "Aquamarine", "Aquamarine Blue", "Arapawa", "Arctic Lime",
            "Armadillo", "Army Green", "Arrowtown", "Arsenic", "Artichoke", "Arylide Yellow", "Ash", "Ash Grey",
            "Asparagus", "Asphalt", "Astra", "Astral", "Astronaut", "Astronaut Blue", "Athens Gray", "Aths Special",
            "Atlantis", "Atoll", "Au Chico", "Aubergine", "Auburn", "Aureolin", "Auro Metal Saurus", "Australian Mint",
            "Avocado", "Axolotl", "Azalea", "Aztec", "Aztec Gold", "Azure", "Azure Mist", "Azureish White", "Baby Blue",
            "Baby Blue Eyes", "Baby Powder", "Bahama Blue", "Bahia", "Baja White", "Baker Miller Pink", "Bali Hai",
            "Ball Blue", "Baltic Sea", "Bamboo", "Banana Mania", "Banana Yellow", "Bandicoot", "Barberry",
            "Barbie Pink", "Barley Corn", "Barley White", "Barn Red", "Barossa", "Bastille", "Battleship Gray",
            "Bay Leaf", "Bay of Many", "Bazaar", "Bdazzled Blue", "Beau Blue", "Beauty Bush", "Beaver", "Beeswax",
            "Beige", "Belgion", "Bermuda", "Bermuda Gray", "Beryl Green", "Bianca", "Big Dip Oruby", "Big Foot Feet",
            "Big Stone", "Bilbao", "Biloba Flower", "Birch", "Bird Flower", "Biscay", "Bismark", "Bison Hide", "Bisque",
            "Bistre", "Bitter", "Bitter Lemon", "Bittersweet", "Bittersweet Shimmer", "Bizarre", "Black", "Black Bean",
            "Black Coral", "Black Forest", "Black Haze", "Black Leather Jacket", "Black Marlin", "Black Olive",
            "Black Pearl", "Black Rock", "Black Rose", "Black Russian", "Black Shadows", "Black Squeeze", "Black White",
            "Blackberry", "Blackcurrant", "Blanched Almond", "Blast Off Bronze", "Blaze Orange", "Bleach White",
            "Bleached Cedar", "Bleu De France", "Blizzard Blue", "Blond", "Blossom", "Blue", "Blue Bayoux", "Blue Bell",
            "Blue Chalk", "Blue Charcoal", "Blue Chill", "Blue Diamond", "Blue Dianne", "Blue Gem", "Blue Gray",
            "Blue Green", "Blue Haze", "Blue Jeans", "Blue Lagoon", "Blue Magenta Violet", "Blue Marguerite",
            "Blue Ribbon", "Blue Romance", "Blue Sapphire", "Blue Smoke", "Blue Stone", "Blue Violet", "Blue Whale",
            "Blue Yonder", "Blue Zodiac", "Blueberry", "Bluebonnet", "Blumine", "Blush", "Bole", "Bombay", "Bon Jour",
            "Bondi Blue", "Bone", "Booger Buster", "Bordeaux", "Bossanova", "Boston Blue", "Boston University Red",
            "Botticelli", "Bottle Green", "Boulder", "Bouquet", "Bourbon", "Boysenberry", "Bracken", "Brandeis Blue",
            "Brandy", "Brandy Punch", "Brandy Rose", "Brass", "Breaker Bay", "Brick Red", "Bridal Heath", "Bridesmaid",
            "Bright Cerulean", "Bright Gray", "Bright Green", "Bright Lavender", "Bright Lilac", "Bright Maroon",
            "Bright Navy Blue", "Bright Red", "Bright Sun", "Bright Turquoise", "Bright Ube", "Bright Yellow",
            "Brilliant Azure", "Brilliant Lavender", "Brilliant Rose", "Brink Pink", "British Racing Green", "Bronco",
            "Bronze", "Bronze Olive", "Bronze Yellow", "Bronzetone", "Broom", "Brown", "Brown Bramble", "Brown Derby",
            "Brown Pod", "Brown Rust", "Brown Sugar", "Brown Tumbleweed", "Brown Yellow", "Brunswick Green",
            "Bubble Gum", "Bubbles", "Buccaneer", "Bud", "Bud Green", "Buddha Gold", "Buff", "Bulgarian Rose",
            "Bull Shot", "Bunker", "Bunting", "Burgundy", "Burlywood", "Burnham", "Burning Orange", "Burning Sand",
            "Burnished Brown", "Burnt Maroon", "Burnt Orange", "Burnt Sienna", "Burnt Umber", "Bush", "Buttercup",
            "Buttered Rum", "Butterfly Bush", "Buttermilk", "Buttery White", "Byzantine", "Byzantium", "CG Blue",
            "CG Red", "Cab Sav", "Cabaret", "Cabbage Pont", "Cactus", "Cadet", "Cadet Blue", "Cadet Grey", "Cadillac",
            "Cadmium Green", "Cadmium Orange", "Cadmium Red", "Cadmium Yellow", "Cafe Noir", "Cafe Royale",
            "Cal Poly Green", "Calico", "California", "Calypso", "Camarone", "Cambridge Blue", "Camelot", "Cameo",
            "Cameo Pink", "Camouflage", "Camouflage Green", "Can Can", "Canary", "Canary Yellow", "Candlelight",
            "Candy Apple Red", "Cannon Black", "Cannon Pink", "Cape Cod", "Cape Honey", "Cape Palliser", "Caper",
            "Capri", "Caput Mortuum", "Caramel", "Cararra", "Cardin Green", "Cardinal", "Cardinal Pink", "Careys Pink",
            "Caribbean Green", "Carissma", "Carla", "Carmine", "Carmine Pink", "Carmine Red", "Carnaby Tan",
            "Carnation", "Carnation Pink", "Carnelian", "Carolina Blue", "Carousel Pink", "Carrot Orange", "Casablanca",
            "Casal", "Cascade", "Cashmere", "Casper", "Castleton Green", "Castro", "Catalina Blue", "Catawba",
            "Catskill White", "Cavern Pink", "Cedar", "Cedar Chest", "Cedar Wood Finish", "Ceil", "Celadon",
            "Celadon Green", "Celery", "Celeste", "Celestial Blue", "Cello", "Celtic", "Cement", "Ceramic", "Cerise",
            "Cerise Pink", "Cerulean", "Cerulean Blue", "Cerulean Frost", "Chablis", "Chalet Green", "Chalky",
            "Chambray", "Chamois", "Chamoisee", "Champagne", "Chantilly", "Charade", "Charcoal", "Chardon",
            "Chardonnay", "Charleston Green", "Charlotte", "Charm", "Charm Pink", "Chartreuse", "Chateau Green",
            "Chatelle", "Chathams Blue", "Chelsea Cucumber", "Chelsea Gem", "Chenin", "Cherokee", "Cherry Blossom Pink",
            "Cherry Pie", "Cherrywood", "Cherub", "Chestnut", "Chetwode Blue", "Chicago", "Chiffon", "Chilean Fire",
            "Chilean Heath", "China Ivory", "China Rose", "Chinese Red", "Chinese Violet", "Chino", "Chinook",
            "Chlorophyll Green", "Chocolate", "Christalle", "Christi", "Christine", "Chrome White", "Chrome Yellow",
            "Cinder", "Cinderella", "Cinereous", "Cinnabar", "Cinnamon Satin", "Cioccolato", "Citrine", "Citrine White",
            "Citron", "Citrus", "Clairvoyant", "Clam Shell", "Claret", "Classic Rose", "Clay Ash", "Clay Creek",
            "Clear Day", "Clementine", "Clinker", "Cloud", "Cloud Burst", "Cloudy", "Clover", "Cobalt Blue",
            "Cocoa Bean", "Cocoa Brown", "Coconut", "Coconut Cream", "Cod Gray", "Coffee", "Coffee Bean", "Cognac",
            "Cola", "Cold Purple", "Cold Turkey", "Colonial White", "Columbia Blue", "Comet", "Como", "Conch",
            "Concord", "Concrete", "Confetti", "Congo Brown", "Congo Pink", "Congress Blue", "Conifer", "Contessa",
            "Cool Black", "Cool Grey", "Copper", "Copper Canyon", "Copper Penny", "Copper Red", "Copper Rose",
            "Copper Rust", "Coquelicot", "Coral", "Coral Red", "Coral Reef", "Coral Tree", "Cordovan", "Corduroy",
            "Coriander", "Cork", "Corn", "Corn Field", "Corn Harvest", "Cornflower Blue", "Cornflower Lilac",
            "Cornsilk", "Corvette", "Cosmic", "Cosmic Cobalt", "Cosmic Latte", "Cosmos", "Costa Del Sol",
            "Cotton Candy", "Cotton Seed", "County Green", "Cowboy", "Coyote Brown", "Crail", "Cranberry",
            "Crater Brown", "Crayola Blue", "Crayola Green", "Crayola Orange", "Crayola Red", "Crayola Yellow", "Cream",
            "Cream Brulee", "Cream Can", "Creole", "Crete", "Crimson", "Crimson Glory", "Crimson Red", "Crocodile",
            "Crown of Thorns", "Crowshead", "Cruise", "Crusoe", "Crusta", "Cumin", "Cumulus", "Cupid", "Curious Blue",
            "Cutty Sark", "Cyan", "Cyan Azure", "Cyan Blue Azure", "Cyan Cobalt Blue", "Cyan Cornflower Blue",
            "Cyber Grape", "Cyber Yellow", "Cyclamen", "Cyprus", "Daffodil", "Daintree", "Dairy Cream", "Daisy Bush",
            "Dallas", "Dandelion", "Danube", "Dark Blue", "Dark Blue Gray", "Dark Brown", "Dark Brown Tangelo",
            "Dark Burgundy", "Dark Byzantium", "Dark Candy Apple Red", "Dark Cerulean", "Dark Chestnut", "Dark Coral",
            "Dark Cyan", "Dark Ebony", "Dark Fern", "Dark Goldenrod", "Dark Green", "Dark Gunmetal",
            "Dark Imperial Blue", "Dark Jungle Green", "Dark Khaki", "Dark Lavender", "Dark Liver", "Dark Magenta",
            "Dark Medium Gray", "Dark Midnight Blue", "Dark Moss Green", "Dark Olive Green", "Dark Orange",
            "Dark Orchid", "Dark Pastel Blue", "Dark Pastel Green", "Dark Pastel Purple", "Dark Pastel Red",
            "Dark Pink", "Dark Puce", "Dark Purple", "Dark Raspberry", "Dark Red", "Dark Salmon", "Dark Scarlet",
            "Dark Sea Green", "Dark Sienna", "Dark Sky Blue", "Dark Slate Blue", "Dark Slate Gray", "Dark Spring Green",
            "Dark Tan", "Dark Tangerine", "Dark Terra Cotta", "Dark Turquoise", "Dark Vanilla", "Dark Violet",
            "Dark Yellow", "Dartmouth Green", "Davys Grey", "Dawn", "Dawn Pink", "De York", "Debian Red", "Deco",
            "Deep Blue", "Deep Blush", "Deep Bronze", "Deep Carmine", "Deep Carmine Pink", "Deep Carrot Orange",
            "Deep Cerise", "Deep Chestnut", "Deep Cove", "Deep Fir", "Deep Forest Green", "Deep Fuchsia", "Deep Green",
            "Deep Green Cyan Turquoise", "Deep Jungle Green", "Deep Koamaru", "Deep Lemon", "Deep Lilac",
            "Deep Magenta", "Deep Maroon", "Deep Oak", "Deep Pink", "Deep Puce", "Deep Red", "Deep Ruby",
            "Deep Saffron", "Deep Sapphire", "Deep Sea", "Deep Sea Green", "Deep Space Sparkle", "Deep Taupe",
            "Deep Teal", "Deep Tuscan Red", "Deep Violet", "Deer", "Del Rio", "Dell", "Delta", "Deluge", "Denim",
            "Denim Blue", "Derby", "Desaturated Cyan", "Desert", "Desert Sand", "Desert Storm", "Desire", "Dew",
            "Di Serria", "Diamond", "Diesel", "Dim Gray", "Dingley", "Dingy Dungeon", "Dirt", "Disco", "Dixie",
            "Dodger Blue", "Dogs", "Dogwood Rose", "Dollar Bill", "Dolly", "Dolphin", "Domino", "Don Juan",
            "Donkey Brown", "Dorado", "Double Colonial White", "Double Pearl Lusta", "Double Spanish White",
            "Dove Gray", "Downriver", "Downy", "Driftwood", "Drover", "Duke Blue", "Dull Lavender", "Dune",
            "Dust Storm", "Dusty Gray", "Dutch White", "Eagle", "Eagle Green", "Earls Green", "Early Dawn",
            "Earth Yellow", "East Bay", "East Side", "Eastern Blue", "Ebb", "Ebony", "Ebony Clay", "Eclipse", "Ecru",
            "Ecru White", "Ecstasy", "Eden", "Edgewater", "Edward", "Eerie Black", "Egg Sour", "Egg White", "Eggplant",
            "Eggshell", "Egyptian Blue", "El Paso", "El Salva", "Electric Blue", "Electric Crimson", "Electric Indigo",
            "Electric Lime", "Electric Purple", "Electric Violet", "Electric Yellow", "Elephant", "Elf Green", "Elm",
            "Emerald", "Eminence", "Emperor", "Empress", "Endeavour", "Energy Yellow",
            "Engineering International Orange", "English Holly", "English Lavender", "English Red",
            "English Vermillion", "English Walnut", "Envy", "Equator", "Espresso", "Eternity", "Eton Blue",
            "Eucalyptus", "Eunry", "Evening Sea", "Everglade", "FOGRA29 Rich Black", "FOGRA39 Rich Black", "Faded Jade",
            "Fair Pink", "Falcon", "Fallow", "Falu Red", "Fandango", "Fandango Pink", "Fantasy", "Fashion Fuchsia",
            "Fawn", "Fedora", "Feijoa", "Feldgrau", "Fern", "Fern Frond", "Fern Green", "Ferra", "Ferrari Red",
            "Festival", "Feta", "Field Drab", "Fiery Orange", "Fiery Rose", "Finch", "Finlandia", "Finn", "Fiord",
            "Fire", "Fire Bush", "Fire Engine Red", "Firebrick", "Firefly", "Flame", "Flame Pea", "Flamenco",
            "Flamingo", "Flamingo Pink", "Flavescent", "Flax", "Flax Smoke", "Flint", "Flirt", "Floral White",
            "Flush Mahogany", "Foam", "Fog", "Foggy Gray", "Folly", "Forest Green", "Forget Me Not", "Fountain Blue",
            "Frangipani", "French Bistre", "French Blue", "French Fuchsia", "French Gray", "French Lilac",
            "French Lime", "French Mauve", "French Pass", "French Pink", "French Plum", "French Puce",
            "French Raspberry", "French Rose", "French Sky Blue", "French Violet", "French Wine", "Fresh Air",
            "Fresh Eggplant", "Friar Gray", "Fringy Flower", "Froly", "Frost", "Frostbite", "Frosted Mint", "Frostee",
            "Fruit Salad", "Fuchsia", "Fuchsia Blue", "Fuchsia Pink", "Fuchsia Purple", "Fuchsia Rose", "Fuego",
            "Fuel Yellow", "Fulvous", "Fun Blue", "Fun Green", "Fuscous Gray", "Fuzzy Wuzzy", "Fuzzy Wuzzy Brown",
            "GO Green", "Gable Green", "Gainsboro", "Gallery", "Galliano", "Gamboge", "Gamboge Orange", "Gargoyle Gas",
            "Geebung", "Generic Viridian", "Genoa", "Geraldine", "Geyser", "Ghost", "Ghost White", "Giants Club",
            "Giants Orange", "Gigas", "Gimblet", "Gin", "Gin Fizz", "Ginger", "Givry", "Glacier", "Glade Green",
            "Glaucous", "Glitter", "Glossy Grape", "Go Ben", "Goblin", "Gold Drop", "Gold Fusion", "Gold Tips",
            "Golden", "Golden Bell", "Golden Brown", "Golden Dream", "Golden Fizz", "Golden Gate Bridge", "Golden Glow",
            "Golden Poppy", "Golden Sand", "Golden Tainoi", "Golden Yellow", "Goldenrod", "Gondola", "Gordons Green",
            "Gorse", "Gossamer", "Gossip", "Gothic", "Governor Bay", "Grain Brown", "Grandis", "Granite Gray",
            "Granite Green", "Granny Apple", "Granny Smith", "Granny Smith Apple", "Grape", "Graphite", "Gravel",
            "Gray", "Gray Asparagus", "Gray Chateau", "Gray Nickel", "Gray Nurse", "Gray Olive", "Gray Suit", "Green",
            "Green Blue", "Green Cyan", "Green Haze", "Green House", "Green Kelp", "Green Leaf", "Green Lizard",
            "Green Mist", "Green Pea", "Green Sheen", "Green Smoke", "Green Spring", "Green Vogue", "Green Waterloo",
            "Green White", "Green Yellow", "Grenadier", "Grizzly", "Grullo", "Guardsman Red", "Gulf Blue",
            "Gulf Stream", "Gull Gray", "Gum Leaf", "Gumbo", "Gun Powder", "Gunmetal", "Gunsmoke", "Gurkha", "Hacienda",
            "Hairy Heath", "Haiti", "Halayà Úbe", "Half Baked", "Half Colonial White", "Half Dutch White",
            "Half Spanish White", "Half and Half", "Hampton", "Han Blue", "Han Purple", "Harlequin", "Harlequin Green",
            "Harp", "Harvard Crimson", "Harvest Gold", "Havelock Blue", "Hawaiian Tan", "Hawkes Blue", "Heat Wave",
            "Heath", "Heather", "Heathered Gray", "Heavy Metal", "Heliotrope", "Heliotrope Gray", "Heliotrope Magenta",
            "Hemlock", "Hemp", "Hibiscus", "Highland", "Hillary", "Himalaya", "Hint of Green", "Hint of Red",
            "Hint of Yellow", "Hippie Blue", "Hippie Green", "Hippie Pink", "Hit Gray", "Hit Pink", "Hokey Pokey",
            "Hoki", "Holly", "Honey Flower", "Honeydew", "Honeysuckle", "Honolulu Blue", "Hookers Green", "Hopbush",
            "Horizon", "Horses", "Horses Neck", "Hot Magenta", "Hot Pink", "Hot Toddy", "Humming Bird", "Hunter Green",
            "Hurricane", "Husk", "Ice Cold", "Iceberg", "Icterine", "Illuminating Emerald", "Illusion", "Imperial",
            "Imperial Blue", "Imperial Red", "Inch Worm", "Inchworm", "Independence", "India Green", "Indian Red",
            "Indian Tan", "Indian Yellow", "Indigo", "Indigo Dye", "Indochine", "International Klein Blue",
            "International Orange", "Iris", "Irish Coffee", "Iroko", "Iron", "Ironside Gray", "Ironstone",
            "Irresistible", "Isabelline", "Islamic Green", "Island Spice", "Ivory", "Jacaranda", "Jacarta",
            "Jacko Bean", "Jacksons Purple", "Jade", "Jaffa", "Jagged Ice", "Jagger", "Jaguar", "Jambalaya", "Janna",
            "Japanese Carmine", "Japanese Indigo", "Japanese Laurel", "Japanese Maple", "Japanese Violet", "Japonica",
            "Jasmine", "Jasper", "Java", "Jazzberry Jam", "Jelly Bean", "Jet", "Jet Stream", "Jewel", "Jon", "Jonquil",
            "Jordy Blue", "Judge Gray", "Jumbo", "June Bud", "Jungle Green", "Jungle Mist", "Juniper", "Just Right",
            "KU Crimson", "Kabul", "Kaitoke Green", "Kangaroo", "Karaka", "Karry", "Kashmir Blue", "Kelly Green",
            "Kelp", "Kenyan Copper", "Keppel", "Key Lime", "Key Lime Pie", "Khaki", "Kidnapper", "Kilamanjaro",
            "Killarney", "Kimberly", "Kingfisher Daisy", "Kobi", "Kobicha", "Kokoda", "Kombu Green", "Korma",
            "Koromiko", "Kournikova", "Kumera", "La Palma", "La Rioja", "La Salle Green", "Languid Lavender",
            "Lapis Lazuli", "Las Palmas", "Laser", "Laurel", "Laurel Green", "Lava", "Lavender", "Lavender Blush",
            "Lavender Gray", "Lavender Indigo", "Lavender Magenta", "Lavender Mist", "Lavender Pink", "Lavender Purple",
            "Lavender Rose", "Lawn Green", "Leather", "Lemon", "Lemon Chiffon", "Lemon Curry", "Lemon Ginger",
            "Lemon Glacier", "Lemon Grass", "Lemon Lime", "Lemon Meringue", "Lemon Yellow", "Lenurple", "Liberty",
            "Licorice", "Light Apricot", "Light Blue", "Light Brilliant Red", "Light Brown", "Light Carmine Pink",
            "Light Cobalt Blue", "Light Coral", "Light Cornflower Blue", "Light Crimson", "Light Cyan",
            "Light Deep Pink", "Light French Beige", "Light Fuchsia Pink", "Light Goldenrod Yellow", "Light Gray",
            "Light Grayish Magenta", "Light Green", "Light Hot Pink", "Light Khaki", "Light Medium Orchid",
            "Light Moss Green", "Light Orchid", "Light Pastel Purple", "Light Pink", "Light Salmon",
            "Light Salmon Pink", "Light Sea Green", "Light Sky Blue", "Light Slate Gray", "Light Steel Blue",
            "Light Taupe", "Light Turquoise", "Light Yellow", "Lightning Yellow", "Lilac", "Lilac Bush", "Lilac Luster",
            "Lily", "Lily White", "Lima", "Lime", "Lime Green", "Limeade", "Limed Ash", "Limed Oak", "Limed Spruce",
            "Limerick", "Lincoln Green", "Linen", "Link Water", "Lipstick", "Lisbon Brown", "Little Boy Blue", "Liver",
            "Liver Chestnut", "Livid Brown", "Loafer", "Loblolly", "Lochinvar", "Lochmara", "Locust", "Log Cabin",
            "Logan", "Lola", "London Hue", "Lonestar", "Lotus", "Loulou", "Lucky", "Lucky Point", "Lumber",
            "Lunar Green", "Lust", "Luxor Gold", "Lynch", "MSU Green", "Mabel", "Macaroni And Cheese",
            "Macaroni and Cheese", "Madang", "Madison", "Madras", "Magenta", "Magenta Haze", "Magenta Pink",
            "Magic Mint", "Magic Potion", "Magnolia", "Mahogany", "Mai Tai", "Maize", "Majorelle Blue", "Makara",
            "Mako", "Malachite", "Malibu", "Mallard", "Malta", "Mamba", "Manatee", "Mandalay", "Mandarin", "Mandy",
            "Mandys Pink", "Mango Tango", "Manhattan", "Mantis", "Mantle", "Manz", "Mardi Gras", "Marigold",
            "Marigold Yellow", "Mariner", "Maroon", "Maroon Oak", "Marshland", "Martini", "Martinique", "Marzipan",
            "Masala", "Matisse", "Matrix", "Matterhorn", "Mauve", "Mauve Taupe", "Mauvelous", "Maverick",
            "Maximum Blue", "Maximum Yellow", "May Green", "Maya Blue", "Meat Brown", "Medium Aquamarine",
            "Medium Blue", "Medium Candy Apple Red", "Medium Electric Blue", "Medium Jungle Green", "Medium Orchid",
            "Medium Purple", "Medium Red Violet", "Medium Ruby", "Medium Sea Green", "Medium Sky Blue",
            "Medium Slate Blue", "Medium Spring Bud", "Medium Spring Green", "Medium Turquoise", "Medium Vermilion",
            "Melanie", "Melanzane", "Mellow Apricot", "Melon", "Melrose", "Mercury", "Merino", "Merlin", "Merlot",
            "Metal Pink", "Metallic Bronze", "Metallic Copper", "Metallic Gold", "Metallic Seaweed",
            "Metallic Sunburst", "Meteor", "Meteorite", "Mexican Pink", "Mexican Red", "Mid Gray", "Midnight",
            "Midnight Blue", "Midnight Moss", "Mikado", "Mikado Yellow", "Milan", "Milano Red", "Milk Punch",
            "Millbrook", "Mimosa", "Mindaro", "Mine Shaft", "Mineral Green", "Ming", "Minion Yellow", "Minsk", "Mint",
            "Mint Cream", "Mint Green", "Mint Julep", "Mint Tulip", "Mirage", "Mischka", "Mist Gray", "Misty Moss",
            "Misty Rose", "Mobster", "Moccaccino", "Moccasin", "Mocha", "Mojo", "Mona Lisa", "Monarch", "Mondo",
            "Mongoose", "Monsoon", "Monte Carlo", "Monza", "Moody Blue", "Moon Glow", "Moon Mist", "Moon Raker",
            "Moonstone Blue", "Mordant Red", "Morning Glory", "Morocco Brown", "Mortar", "Mosque", "Moss Green",
            "Mountain Meadow", "Mountain Mist", "Mountbatten Pink", "Muddy Waters", "Muesli", "Mughal Green",
            "Mulberry", "Mulberry Wood", "Mule Fawn", "Mulled Wine", "Mummys Tomb", "Munsell Blue", "Munsell Green",
            "Munsell Purple", "Munsell Red", "Munsell Yellow", "Mustard", "My Pink", "My Sin", "Myrtle Green", "Mystic",
            "Mystic Maroon", "NCS Blue", "NCS Green", "NCS Red", "Nadeshiko Pink", "Nandor", "Napa", "Napier Green",
            "Naples Yellow", "Narvik", "Natural Gray", "Navajo White", "Navy", "Nebula", "Negroni", "Neon Carrot",
            "Neon Fuchsia", "Neon Green", "Nepal", "Neptune", "Nero", "Nevada", "New Car", "New Orleans",
            "New York Pink", "Niagara", "Nickel", "Night Rider", "Night Shadz", "Nile Blue", "Nobel", "Nomad",
            "Non Photo Blue", "North Texas Green", "Norway", "Nugget", "Nutmeg", "Nutmeg Wood Finish", "Nyanza",
            "Oasis", "Observatory", "Ocean Blue", "Ocean Boat Blue", "Ocean Green", "Ochre", "Off Green", "Off Yellow",
            "Ogre Odor", "Oil", "Old Brick", "Old Burgundy", "Old Copper", "Old Gold", "Old Heliotrope", "Old Lace",
            "Old Lavender", "Old Moss Green", "Old Rose", "Old Silver", "Olive", "Olive Drab", "Olive Drab Seven",
            "Olive Green", "Olive Haze", "Olivetone", "Olivine", "Onahau", "Onion", "Onyx", "Opal", "Opera Mauve",
            "Opium", "Oracle", "Orange", "Orange Peel", "Orange Red", "Orange Roughy", "Orange Soda", "Orange White",
            "Orange Yellow", "Orchid", "Orchid Pink", "Orchid White", "Oregon", "Organ", "Orient", "Oriental Pink",
            "Orinoco", "Orioles Orange", "Oslo Gray", "Ottoman", "Outer Space", "Outrageous Orange", "Oxford Blue",
            "Oxley", "Oyster Bay", "Oyster Pink", "Paarl", "Pablo", "Pacific Blue", "Pacifika", "Paco", "Padua",
            "Pakistan Green", "Palatinate Blue", "Palatinate Purple", "Pale Brown", "Pale Canary", "Pale Carmine",
            "Pale Cerulean", "Pale Chestnut", "Pale Copper", "Pale Cornflower Blue", "Pale Cyan", "Pale Gold",
            "Pale Goldenrod", "Pale Green", "Pale Lavender", "Pale Leaf", "Pale Magenta", "Pale Magenta Pink",
            "Pale Oyster", "Pale Pink", "Pale Plum", "Pale Prim", "Pale Red Violet", "Pale Robin Egg Blue", "Pale Rose",
            "Pale Silver", "Pale Sky", "Pale Slate", "Pale Spring Bud", "Pale Taupe", "Pale Violet", "Palm Green",
            "Palm Leaf", "Pampas", "Panache", "Pancho", "Pansy Purple", "Pantone Blue", "Pantone Green",
            "Pantone Magenta", "Pantone Orange", "Pantone Pink", "Pantone Yellow", "Paolo Veronese Green",
            "Papaya Whip", "Paprika", "Paradise Pink", "Paradiso", "Parchment", "Paris Daisy", "Paris M", "Paris White",
            "Parsley", "Pastel Blue", "Pastel Brown", "Pastel Gray", "Pastel Green", "Pastel Magenta", "Pastel Orange",
            "Pastel Pink", "Pastel Purple", "Pastel Red", "Pastel Violet", "Pastel Yellow", "Patina", "Pattens Blue",
            "Paua", "Pavlova", "Paynes Grey", "Peach", "Peach Cream", "Peach Orange", "Peach Puff", "Peach Schnapps",
            "Peach Yellow", "Peanut", "Pear", "Pearl", "Pearl Aqua", "Pearl Bush", "Pearl Lusta",
            "Pearl Mystic Turquoise", "Pearly Purple", "Peat", "Pelorous", "Peppermint", "Perano", "Perfume", "Peridot",
            "Periglacial Blue", "Periwinkle", "Periwinkle Gray", "Permanent Geranium Lake", "Persian Blue",
            "Persian Green", "Persian Indigo", "Persian Orange", "Persian Pink", "Persian Plum", "Persian Red",
            "Persian Rose", "Persimmon", "Peru", "Peru Tan", "Pesto", "Petite Orchid", "Pewter", "Pewter Blue",
            "Pharlap", "Phthalo Blue", "Phthalo Green", "Picasso", "Pickled Bean", "Pickled Bluewood", "Picton Blue",
            "Pictorial Carmine", "Pig Pink", "Pigeon Post", "Piggy Pink", "Pigment Blue", "Pigment Green",
            "Pigment Red", "Pine Cone", "Pine Glade", "Pine Green", "Pine Tree", "Pink", "Pink Flamingo", "Pink Flare",
            "Pink Lace", "Pink Lady", "Pink Lavender", "Pink Orange", "Pink Pearl", "Pink Raspberry", "Pink Sherbet",
            "Pink Swan", "Piper", "Pipi", "Pippin", "Pirate Gold", "Pistachio", "Pixie Green", "Pixie Powder", "Pizazz",
            "Pizza", "Plantation", "Platinum", "Plum", "Plump Purple", "Pohutukawa", "Polar", "Polished Pine",
            "Polo Blue", "Pomegranate", "Pompadour", "Popstar", "Porcelain", "Porsche", "Port Gore", "Portafino",
            "Portage", "Portica", "Portland Orange", "Pot Pourri", "Potters Clay", "Powder Ash", "Powder Blue",
            "Prairie Sand", "Prelude", "Prim", "Primrose", "Princess Perfume", "Princeton Orange", "Process Cyan",
            "Process Magenta", "Provincial Pink", "Prussian Blue", "Psychedelic Purple", "Puce", "Pueblo",
            "Puerto Rico", "Pullman Brown", "Pullman Green", "Pumice", "Pumpkin", "Pumpkin Skin", "Punch", "Punga",
            "Purple", "Purple Heart", "Purple Mountain Majesty", "Purple Navy", "Purple Pizzazz", "Purple Plum",
            "Purple Taupe", "Purpureus", "Putty", "Quarter Pearl Lusta", "Quarter Spanish White", "Quartz",
            "Queen Blue", "Queen Pink", "Quick Silver", "Quicksand", "Quill Gray", "Quinacridone Magenta", "Quincy",
            "RYB Blue", "RYB Green", "RYB Orange", "RYB Red", "RYB Violet", "RYB Yellow", "Racing Green", "Radical Red",
            "Raffia", "Rainee", "Raisin Black", "Rajah", "Rangitoto", "Rangoon Green", "Raspberry", "Raspberry Pink",
            "Raven", "Raw Sienna", "Raw Umber", "Razzle Dazzle Rose", "Razzmatazz", "Razzmic Berry", "Rebecca Purple",
            "Rebel", "Red", "Red Beech", "Red Berry", "Red Damask", "Red Devil", "Red Orange", "Red Oxide",
            "Red Purple", "Red Ribbon", "Red Robin", "Red Salsa", "Red Stage", "Red Violet", "Redwood", "Reef",
            "Reef Gold", "Regal Blue", "Regalia", "Regent Gray", "Regent St Blue", "Remy", "Reno Sand",
            "Resolution Blue", "Revolver", "Rhino", "Rhythm", "Rice Cake", "Rice Flower", "Rich Black",
            "Rich Brilliant Lavender", "Rich Carmine", "Rich Electric Blue", "Rich Gold", "Rich Lavender", "Rich Lilac",
            "Rich Maroon", "Rifle Green", "Rio Grande", "Ripe Lemon", "Ripe Plum", "Riptide", "River Bed",
            "Roast Coffee", "Rob Roy", "Robin Egg Blue", "Rock", "Rock Blue", "Rock Spray", "Rocket Metallic",
            "Rodeo Dust", "Rolling Stone", "Roman", "Roman Coffee", "Roman Silver", "Romance", "Romantic", "Ronchi",
            "Roof Terracotta", "Rope", "Rose", "Rose Bonbon", "Rose Bud", "Rose Bud Cherry", "Rose Dust", "Rose Ebony",
            "Rose Fog", "Rose Gold", "Rose Pink", "Rose Red", "Rose Taupe", "Rose Vale", "Rose White", "Rose of Sharon",
            "Rosewood", "Rosso Corsa", "Rosy Brown", "Roti", "Rouge", "Royal Air Force Blue", "Royal Azure",
            "Royal Blue", "Royal Fuchsia", "Royal Heath", "Royal Purple", "Ruber", "Rubine Red", "Ruby", "Ruby Red",
            "Ruddy", "Ruddy Brown", "Ruddy Pink", "Rufous", "Rum", "Rum Swizzle", "Russet", "Russett", "Russian Green",
            "Russian Violet", "Rust", "Rustic Red", "Rusty Nail", "Rusty Red", "SAE ECE Amber",
            "Sacramento State Green", "Saddle", "Saddle Brown", "Safety Orange", "Safety Yellow", "Saffron",
            "Saffron Mango", "Sage", "Sahara", "Sahara Sand", "Sail", "Salem", "Salmon", "Salmon Pink", "Salomie",
            "Salt Box", "Saltpan", "Sambuca", "San Felix", "San Juan", "San Marino", "Sand Dune", "Sandal", "Sandrift",
            "Sandstone", "Sandstorm", "Sandwisp", "Sandy Beach", "Sandy Brown", "Sangria", "Sanguine Brown", "Santa Fe",
            "Santas Gray", "Sap Green", "Sapling", "Sapphire", "Sapphire Blue", "Saratoga", "Sasquatch Socks",
            "Satin Linen", "Satin Sheen Gold", "Sauvignon", "Sazerac", "Scampi", "Scandal", "Scarlet", "Scarlet Gum",
            "Scarlett", "Scarpa Flow", "Schist", "School Bus Yellow", "Schooner", "Science Blue", "Scooter", "Scorpion",
            "Scotch Mist", "Screamin Green", "Sea Blue", "Sea Buckthorn", "Sea Green", "Sea Mist", "Sea Nymph",
            "Sea Pink", "Sea Serpent", "Seagull", "Seal Brown", "Seance", "Seashell", "Seaweed", "Selago",
            "Selective Yellow", "Sepia", "Sepia Black", "Sepia Skin", "Serenade", "Shadow", "Shadow Blue",
            "Shadow Green", "Shady Lady", "Shakespeare", "Shalimar", "Shampoo", "Shamrock", "Shamrock Green", "Shark",
            "Sheen Green", "Sherpa Blue", "Sherwood Green", "Shilo", "Shimmering Blush", "Shingle Fawn",
            "Shiny Shamrock", "Ship Cove", "Ship Gray", "Shiraz", "Shocking", "Shocking Pink", "Shuttle Gray", "Siam",
            "Sidecar", "Sienna", "Silk", "Silver", "Silver Chalice", "Silver Lake Blue", "Silver Pink", "Silver Sand",
            "Silver Tree", "Sinbad", "Sinopia", "Siren", "Sirocco", "Sisal", "Sizzling Red", "Sizzling Sunrise",
            "Skeptic", "Skobeloff", "Sky Blue", "Sky Magenta", "Slate Blue", "Slate Gray", "Slimy Green", "Smalt",
            "Smalt Blue", "Smashed Pumpkin", "Smitten", "Smoke", "Smokey Topaz", "Smoky", "Smoky Black", "Smoky Topaz",
            "Snow", "Snow Drift", "Snow Flurry", "Snowy Mint", "Snuff", "Soap", "Soapstone", "Soft Amber", "Soft Peach",
            "Solid Pink", "Solitaire", "Solitude", "Sonic Silver", "Sorbus", "Sorrell Brown", "Soya Bean",
            "Space Cadet", "Spanish Bistre", "Spanish Blue", "Spanish Carmine", "Spanish Crimson", "Spanish Gray",
            "Spanish Green", "Spanish Orange", "Spanish Pink", "Spanish Red", "Spanish Sky Blue", "Spanish Violet",
            "Spanish Viridian", "Spartan Crimson", "Spectra", "Spice", "Spicy Mix", "Spicy Mustard", "Spicy Pink",
            "Spindle", "Spiro Disco Ball", "Spray", "Spring Bud", "Spring Frost", "Spring Green", "Spring Leaves",
            "Spring Rain", "Spring Sun", "Spring Wood", "Sprout", "Spun Pearl", "Squirrel", "St Patricks Blue",
            "St Tropaz", "Stack", "Star Command Blue", "Star Dust", "Stark White", "Starship", "Steel Blue",
            "Steel Gray", "Steel Pink", "Steel Teal", "Stiletto", "Stonewall", "Storm Dust", "Storm Gray", "Stormcloud",
            "Stratos", "Straw", "Strawberry", "Strikemaster", "Stromboli", "Studio", "Submarine", "Sugar Cane",
            "Sugar Plum", "Sulu", "Summer Green", "Sun", "Sunburnt Cyclops", "Sundance", "Sundown", "Sunflower",
            "Sunglo", "Sunglow", "Sunny", "Sunray", "Sunset", "Sunset Orange", "Sunshade", "Super Pink", "Supernova",
            "Surf", "Surf Crest", "Surfie Green", "Sushi", "Suva Gray", "Swamp", "Swamp Green", "Swans Down",
            "Sweet Brown", "Sweet Corn", "Sweet Pink", "Swirl", "Swiss Coffee", "Sycamore", "Tabasco", "Tacao", "Tacha",
            "Tahiti Gold", "Tahuna Sands", "Tall Poppy", "Tallow", "Tamarillo", "Tamarind", "Tan", "Tan Hide", "Tana",
            "Tangaroa", "Tangelo", "Tangerine", "Tangerine Yellow", "Tango", "Tango Pink", "Tapa", "Tapestry", "Tara",
            "Tarawera", "Tart Orange", "Tasman", "Taupe", "Taupe Gray", "Tawny Port", "Te Papa Green", "Tea",
            "Tea Green", "Tea Rose", "Teak", "Teal", "Teal Blue", "Teal Deer", "Teal Green", "Telemagenta", "Temptress",
            "Tenne", "Tequila", "Terra Cotta", "Texas", "Texas Rose", "Thatch", "Thatch Green", "Thistle",
            "Thistle Green", "Thulian Pink", "Thunder", "Thunderbird", "Tia Maria", "Tiara", "Tiber", "Tickle Me Pink",
            "Tidal", "Tide", "Tiffany Blue", "Tigers Eye", "Timber Green", "Timberwolf", "Titan White",
            "Titanium Yellow", "Toast", "Tobacco Brown", "Toledo", "Tolopea", "Tom Thumb", "Tomato", "Tonys Pink",
            "Toolbox", "Topaz", "Torea Bay", "Tory Blue", "Tosca", "Totem Pole", "Tower Gray", "Tractor Red",
            "Tradewind", "Tranquil", "Travertine", "Tree Poppy", "Treehouse", "Trendy Green", "Trendy Pink", "Trinidad",
            "Tropical Blue", "Tropical Rain Forest", "Tropical Violet", "Trout", "True Blue", "True V", "Tuatara",
            "Tuft Bush", "Tufts Blue", "Tulip", "Tulip Tree", "Tumbleweed", "Tuna", "Tundora", "Turbo", "Turkish Rose",
            "Turmeric", "Turquoise", "Turquoise Blue", "Turquoise Green", "Turtle Green", "Tuscan Red", "Tuscan Tan",
            "Tuscany", "Tusk", "Tussock", "Tutu", "Twilight", "Twilight Blue", "Twilight Lavender", "Twine",
            "Tyrian Purple", "UA Blue", "UA Red", "UCLA Blue", "UCLA Gold", "UFO Green", "UP Forest Green", "UP Maroon",
            "USAFA Blue", "Ube", "Ultra Pink", "Ultramarine", "Ultramarine Blue", "Umber", "Unbleached Silk",
            "Underage Pink", "United Nations Blue", "University Of California Gold", "University Of Tennessee Orange",
            "Unmellow Yellow", "Upsdell Red", "Urobilin", "Utah Crimson", "Valencia", "Valentino", "Valhalla",
            "Van Cleef", "Van Dyke Brown", "Vanilla", "Vanilla Ice", "Varden", "Vegas Gold", "Venetian Red",
            "Venice Blue", "Venus", "Verdigris", "Verdun Green", "Vermilion", "Veronica", "Very Light Azure",
            "Very Light Blue", "Very Light Malachite Green", "Very Light Tangelo", "Very Pale Orange",
            "Very Pale Yellow", "Vesuvius", "Victoria", "Vida Loca", "Viking", "Vin Rouge", "Viola", "Violent Violet",
            "Violet", "Violet Blue", "Violet Eggplant", "Violet Red", "Viridian", "Viridian Green", "Vis Vis",
            "Vista Blue", "Vista White", "Vivid Amber", "Vivid Auburn", "Vivid Burgundy", "Vivid Cerise",
            "Vivid Cerulean", "Vivid Crimson", "Vivid Gamboge", "Vivid Lime Green", "Vivid Malachite", "Vivid Mulberry",
            "Vivid Orange", "Vivid Orange Peel", "Vivid Orchid", "Vivid Raspberry", "Vivid Red", "Vivid Red Tangelo",
            "Vivid Sky Blue", "Vivid Tangelo", "Vivid Tangerine", "Vivid Vermilion", "Vivid Violet", "Vivid Yellow",
            "Volt", "Voodoo", "Vulcan", "Wafer", "Waikawa Gray", "Waiouru", "Walnut", "Warm Black", "Wasabi",
            "Water Leaf", "Watercourse", "Waterloo ", "Waterspout", "Wattle", "Watusi", "Wax Flower", "We Peep",
            "Web Chartreuse", "Web Orange", "Wedgewood", "Weldon Blue", "Well Read", "Wenge", "West Coast", "West Side",
            "Westar", "Wewak", "Wheat", "Wheatfield", "Whiskey", "Whisper", "White", "White Ice", "White Lilac",
            "White Linen", "White Pointer", "White Rock", "White Smoke", "Wild Blue Yonder", "Wild Orchid", "Wild Rice",
            "Wild Sand", "Wild Strawberry", "Wild Watermelon", "Wild Willow", "William", "Willow Brook", "Willow Grove",
            "Willpower Orange", "Windsor", "Windsor Tan", "Wine", "Wine Berry", "Wine Dregs", "Winter Hazel",
            "Winter Sky", "Winter Wizard", "Wintergreen Dream", "Wisp Pink", "Wisteria", "Wistful", "Witch Haze",
            "Wood Bark", "Woodland", "Woodrush", "Woodsmoke", "Woody Brown", "X11 Dark Green", "X11 Gray", "Xanadu",
            "Yale Blue", "Yankees Blue", "Yellow", "Yellow Green", "Yellow Metal", "Yellow Orange", "Yellow Rose",
            "Yellow Sea", "Your Pink", "Yukon Gold", "Yuma", "Zaffre", "Zambezi", "Zanah", "Zest", "Zeus", "Ziggurat",
            "Zinnwaldite", "Zinnwaldite Brown", "Zircon", "Zombie", "Zomp", "Zorba", "Zuccini", "Zumthor" };

    public String[] fashionBrands = { "Roadster", "LOCOMOTIVE", "Zivame", "Mast & Harbour", "HIGHLANDER", "Mayra", "HERE&NOW",
            "HRX by Hrithik Roshan", "Anubhutee", "Athena", "Vishudh", "Sangria", "Tokyo Talkies", "DressBerry",
            "Anouk", "Enamor", "all about you", "KASSUALLY", "RARE", "Zastraa", "WISSTLER", "Cottinfab", "QUIERO",
            "ELEVANTO", "WROGN", "SASSAFRAS", "her by invictus", "StyleStone", "Moda Rapido", "Harpa",
            "Kook N Keech Marvel", "plusS", "La Zoire", "anayna", "Azira", "Taavi", "Varanga", "STREET 9", "Levis",
            "Dollar Missy", "Trend Arrest", "Tikhi Imli", "Belle Fille", "ether", "Jockey", "Jaipur Kurti", "Yuris",
            "Claura", "Softrose", "NEU LOOK FASHION", "Campus Sutra", "Kook N Keech", "Sera", "Peter England", "H&M",
            "Antheaa", "Hubberholme", "Clora Creation", "mokshi", "Saree mall", "Lebami", "MISH", "VIMAL", "I like me",
            "Marie Claire", "Go Colors", "JAINISH", "Dennis Lingo", "ETC", "Clovia", "Roadster Fast and Furious",
            "Purple Feather", "SCORPIUS", "DOLCE CRUDO", "Chkokko", "Floret", "C9 AIRWEAR", "Domyos By Decathlon",
            "U.S. Polo Assn.", "kipek", "U&F", "KALINI", "Harvard", "Ishin", "Berrylush", "Inddus", "Hangup",
            "URBAN SCOTTISH", "Janasya", "Miss Chase", "XYXX", "Bewakoof", "Indo Era", "AHIKA", "TAG 7", "Myshka",
            "DILLINGER", "one8 by Virat Kohli", "Laceandme", "Rosaline by Zivame", "Bhama Couture", "Veni Vidi Vici",
            "Maniac", "Ahalyaa", "Juniper", "Kotty", "See Designs", "VEIRDO", "INVICTUS", "TWIN BIRDS", "High Star",
            "Mitera", "Chemistry", "Hypernation", "Nayo", "Libas", "Rigo", "DEYANN", "Indian Virasat", "Tissu",
            "ABELINO", "Black coffee", "aasi", "Louis Philippe Sport", "Shae by SASSAFRAS", "Artengo By Decathlon",
            "House of Pataudi", "Prakhya", "GoSriKi", "Kook N Keech Disney", "DEEBACO", "KSUT", "Smarty Pants",
            "Stylum", "DENNISON", "Lady Lyka", "FashionRack", "Kook N Keech Superman", "Shaily", "Amante",
            "Boston Club", "Ruhaans", "ZIYAA", "NYAMBA By Decathlon", "Purple State", "Florence", "Carlton London",
            "Yufta", "Varkala Silk Sarees", "Genius18", "Kanvin", "Popnetic", "Jack & Jones", "Bushirt", "Hancock",
            "Slenor", "Quechua By Decathlon", "Os", "Kipsta By Decathlon", "IVOC", "Jompers", "Leading Lady",
            "Ira Soleil", "Bitterlime", "PUMA Motorsport", "SOJANYA", "Meeranshi", "English Navy", "Cross Court",
            "Deewa", "Secret Wish", "Van Heusen", "Zima Leto", "Idalia", "YASH GALLERY", "RANGMANCH BY PANTALOONS",
            "JUNEBERRY", "The Indian Garage Co", "Aujjessa", "Bene Kleed", "Cation", "Poshak Hub", "Nike", "VividArtsy",
            "OM SAI LATEST CREATION", "Pepe Jeans", "Quttos", "Dupatta Bazaar", "W", "ARISE", "RARE ROOTS",
            "WROGN ACTIVE", "Friskers", "River Of Design Jeans", "Rain & Rainbow", "PrettyCat", "WoowZerz",
            "The Dry State", "Uptownie Lite", "Masha", "Kalenji By Decathlon", "THREAD MUSTER", "Huetrap",
            "MABISH by Sonal Jain", "Style Quotient", "Justice League", "Color Cocktail", "Aayna", "WEDZE By Decathlon",
            "PURYS", "Slumber Jill", "MomToBe", "Rajnandini", "KOPNHAGN", "WILD WEST", "DAMENSCH", "Sringam", "Satrani",
            "DRAPE IN VOGUE", "Puma", "Studio Shringaar", "PHWOAR", "AASI - HOUSE OF NAYO", "SOLOGNAC By Decathlon",
            "ANAISA", "20Dresses", "VASTRANAND", "THE BEAR HOUSE", "Sztori", "Kryptic", "FABRIC FITOOR",
            "Difference of Opinion", "Maaesa", "Khushal K", "FIDO DIDO", "KIPRUN By Decathlon", "DIVYANK",
            "Karmic Vision", "Urbano Fashion", "Nation Polo Club", "Imfashini", "Breakbounce", "Dollar Bigboss",
            "mod & shy", "GERUA", "Bitz", "PARIS HAMILTON", "Cultsport", "max", "Minions by Kook N Keech",
            "FORCLAZ By Decathlon", "Shaftesbury London", "ADIDAS", "Dexter by Kook N Keech", "Flying Machine",
            "ROVING MODE", "BULLMER", "MANGO", "SHAVYA", "Benstoke", "Supersox", "EXTRA LOVE BY LIBAS", "Triumph",
            "Vero Moda", "PANIT", "Napra", "zebu", "NAVYSPORT", "MsFQ", "The Vanca", "aamna", "Klamotten", "KISAH",
            "Allen Solly Sport", "ARMISTO", "Biba", "PERFKT-U", "Austin wood", "Leather Retail", "Disney by DressBerry",
            "Bodycare", "FASHION DEPTH", "Balenzia", "Powerpuff Girls by Kook N Keech", "Dollar",
            "BYFORD by Pantaloons", "SALWAR STUDIO", "QOMN", "Besiva", "Pavechas", "Indibelle", "Zeyo", "Laabha",
            "Espresso", "U.S. Polo Assn. Denim Co.", "MBE", "AURELIA", "Code 61", "Zelocity by Zivame", "Da Intimo",
            "Urban Dog", "JAIPUR ATTIRE", "Amydus", "PORTBLAIR", "Just Wow", "PrettySecrets", "John Pride",
            "FableStreet", "Chemistry Edition", "SECRETS BY ZEROKAATA", "Inesis By Decathlon", "Rajesh Silk Mills",
            "Rodamo", "Kook N Keech Star Wars", "HOUSE OF KKARMA", "Prakrti", "Soie", "Tulsattva", "Allen Solly",
            "98 Degree North", "FEMEA", "Sand Dune", "Divena", "CUKOO", "Innocence", "Van Heusen Woman", "Vemante",
            "Rustorange", "Rute", "SIRIKIT", "Mr Bowerbird", "ONLY", "mf", "Ginger by Lifestyle", "ManQ CASUAL",
            "United Colors of Benetton", "InWeave", "GRACIT", "M&H Easy", "Ives", "SHAYE", "SQew", "De Moza", "Vbuyz",
            "Louis Philippe Jeans", "VIMAL JONNEY", "Tinted", "Ojjasvi", "FabAlley", "Kvsfab", "Kalt", "Pannkh",
            "Proline Active", "SWAGG INDIA", "Nifty", "DOOR74", "Louis Philippe", "Qurvii", "PURVAJA", "ROMEO ROSSI",
            "Nite Flite", "Kalista", "Orchid Blues", "INDYA", "Red Tape", "Ethnic basket", "AND", "Eavan", "Kappa",
            "EthnoVogue", "Melange by Lifestyle", "Revolution", "DIXCY SCOTT", "COBB", "IMARA", "Bonjour", "Aasiya",
            "MANQ", "Kraus Jeans", "ANVI Be Yourself", "Vastraa Fusion", "Fasense", "bigbanana", "studio rasa",
            "one8 x PUMA", "IC4", "Foreign Culture By Fort Collins", "MAG", "V&M", "JUMP USA", "TOM BURG", "inocenCia",
            "Pretty Awesome", "SheWill", "M&H Our Water", "Nanda Silk Mills", "ONN", "Minions by Dressberry",
            "KROSSSTITCH", "Marks & Spencer", "WineRed", "Blush 9 Maternity", "PRETTYBOLD", "One8", "SHUBHKALA",
            "RAISIN", "Manyavar", "Joven", "Karigari", "Indietoga", "FASHOR", "Get Glamr", "Dreamz by Pantaloons",
            "MAGRE", "PAUSE SPORT", "Lee", "Oxolloxo", "FABNEST", "Yuuki", "Amrutam Fab",
            "Johnny Bravo by Kook N Keech", "MIMOSA", "BUKKUM", "M7 by Metronaut", "VASTRAMAY",
            "Calvin Klein Underwear", "Dollar Ultra", "Steenbok", "GRITSTONES", "CODE by Lifestyle", "EROTISSCH",
            "Trident", "Inner Sense", "BROADSTAR", "GULMOHAR JAIPUR", "Wonder Woman", "Dynamocks", "The Mom Store",
            "DIXCY SCOTT MAXIMUS", "NEVA", "Arrow", "hangup trend", "Silk Bazar", "TRENDY DIVVA", "OFF LIMITS",
            "Reebok", "Club York", "Nejo", "N-Gal", "RAJUBHAI HARGOVINDAS", "Castle", "Ethnic Junction", "Alcis",
            "Slazenger", "SHINOY", "UNISETS", "Fbella", "Urbano Plus", "MELOSO", "Nautica", "SECRETT CURVES",
            "Blackberrys", "BREIL BY FORT COLLINS", "Kook N Keech Batman", "Texco", "RedRound", "VOXATI", "Richlook",
            "SAPPER", "NAMASKAR", "Rue Collection", "N2S NEXT2SKIN", "heemara", "Molly & Michel", "SPYKAR", "Colt",
            "Souchii", "Wear Your Mind", "OLAIAN By Decathlon", "The Souled Store", "Desi Weavess", "appulse", "AASK",
            "Allen Solly Woman", "Ultimo", "ZHEIA", "MIRCHI FASHION", "FabSeasons", "Wrangler", "FUGAZEE", "KETCH",
            "GESPO", "7Threads", "NEUDIS", "Free Authority", "panchhi", "Force NXT", "Sugathari", "TERQUOIS",
            "WEAVERS VILLA", "I Know", "even", "Wildcraft", "Zigo", "Fabriko", "Plagg", "AARA", "Nun", "beevee",
            "MBeautiful", "Invictus Indoor", "SINGLE", "SOUNDARYA", "INDE by MISH", "TABARD", "Vami", "LOOKNBOOK ART",
            "Souminie", "Global Desi", "Curare", "RANGMAYEE", "Ruggers", "skyria", "Blinkin", "TRIBAN By Decathlon",
            "AKIMIA", "Ajile by Pantaloons", "TREEMODA", "Globon Impex", "Sztori Garfield", "Tweens", "Iti",
            "American Crew", "SIAH", "Indian Women", "RICHARD PARKER by Pantaloons", "Sztori Minions", "LE BOURGEOIS",
            "Bellofox", "TULIP 21", "ZOEYAMS", "Saffron Threads", "Suta", "Miaz Lifestyle", "WESTCLO", "ELEGANCE",
            "Alena", "CINOCCI", "513", "Peter England Casuals", "BlissClub", "Fort Collins",
            "indus route by Pantaloons", "People", "Ginni Arora Label", "Sztori Disney", "Justanned",
            "WATKO By Decathlon", "LOOM LEGACY", "WISHFUL", "ADORENITE", "mackly", "CREEZ", "Sanwara", "I AM FOR YOU",
            "Van Heusen Sport", "AgrohA", "LILL", "Saanjh", "GRIFFEL", "Bannos Swagger", "Sports52 wear",
            "Calvin Klein Jeans", "mezmoda", "Chhabra 555", "Flambeur", "Arrow New York", "Rudra Bazaar",
            "Swee Shapewear", "Nehamta", "YAK YAK", "The Pink Moon", "Arrow Sport", "Classic Polo", "QUARANTINE",
            "Rosaline", "Rubans", "V TRADITION", "Bewakoof Plus", "LAMAAYA", "Raymond", "STALK", "FLX By Decathlon",
            "Centra", "Kazo", "Magnetic Designs", "Marc Loire", "RUF & TUF", "Readiprint Fashions", "JISORA", "2GO",
            "Urban Ranger by pantaloons", "Xpose", "V Dot", "Nabaiji By Decathlon", "ADIDAS Originals", "Lady Love",
            "Park Avenue Woman", "Mode by Red Tape", "Tommy Hilfiger", "RG DESIGNERS", "FAVOROSKI", "DIVASTRI",
            "MPL SPORTS", "Akshatani", "Cayman", "Aprique FAB", "Mameraa", "Saadgi", "J Style", "ZOLA", "Emmyrobe",
            "Lux Cozi", "Ethnicity", "TARMAK By Decathlon", "SCOUP", "Monte Carlo", "Indian Terrain", "SHOPGARB",
            "FurryFlair", "Zeel", "Plutus", "HPS Sports", "Tribord By Decathlon", "Silai Bunai", "Curvy Love",
            "Instafab Plus", "FUAARK", "DC by Kook N Keech", "BRINNS", "ZIZO By Namrata Bajaj", "URBANIC", "Molcha",
            "Rajnie", "Honey by Pantaloons", "ETHNIC STREET", "FREECULTR", "Anoma", "every de by amante",
            "Sweet Dreams", "Darzi", "Freehand", "LADUSAA", "PARFAIT", "Aaruvi Ruchi Verma", "Laado - Pamper Yourself",
            "Ode by House of Pataudi", "SWISS MILITARY", "FOUGANZA By Decathlon", "VAABA", "John Players", "PIROH",
            "RARE RABBIT", "Coucou by Zivame", "Malachi", "VRSALES", "JATRIQQ", "Vento", "Park Avenue", "Aarrah",
            "SLICKFIX", "LUX Cozi for her", "NOT YET by us", "FCUK", "Conditions Apply", "Aawari", "Llak Jeans",
            "Man Matters", "Abiti Bella", "FOREVER 21", "Candyskin", "Okane", "Hush Puppies", "Bossini", "Triveni",
            "DRAAX Fashions", "DODO & MOA", "AKKRITI BY PANTALOONS", "CHILL WINSTON", "ANMI", "Palakh", "MAMMA PRESTO",
            "RAJGRANTH", "UnderJeans by Spykar", "hummel", "KATSO", "Seven Rocks", "Sonari", "Cantabil",
            "HIGHLIGHT FASHION EXPORT", "C9", "Duke", "Being Human", "BODYCARE INSIDER", "Enchanted Drapes", "laavian",
            "Wintage", "Camey", "UNSULLY", "saubhagya", "One Femme", "Globus", "t-base", "Yuvraah", "ADBUCKS",
            "Aditi Wasan", "Swishchick", "Kook N Keech Emoji", "Manthan", "Martini", "TARAMA", "FEVER",
            "Allen Solly Tribe", "TAG 7 PLUS", "BRAG", "Rangriti", "TANKHI", "Candour London", "Pashmoda", "MADSTO",
            "Fitkin", "Louis Philippe Ath.Work", "Forever New", "Looney Tunes By Sztori", "Ducati", "FCUK Underwear",
            "Celio", "ASICS", "antaran", "Dermawear", "Off Label", "clorals", "Pisara", "GOLDSTROMS", "Ayaany",
            "Enviously Young", "QUANCIOUS", "Excalibur", "flaher", "Powerpuff Girls by Dressberry", "Power", "Sugr",
            "Grancy", "Wacoal", "COVER STORY", "Bronz", "PETER ENGLAND UNIVERSITY", "XIN", "Sasimo", "Vartah",
            "Kook N Keech Wonder Woman", "JC Collection", "Chennis", "beebelle", "An Episode", "Femella", "Lee Cooper",
            "Geroo Jaipur", "AV2", "Skechers", "Blissta", "DESI WOMANIYA", "Chumbak", "Maanja", "BRACHY",
            "DIVA WALK EXCLUSIVE", "Cherokee", "Sakhi Jaipur", "Kook N Keech Toons", "Annabelle by Pantaloons",
            "MONOCHROME", "DECHEN", "AMERICAN EAGLE OUTFITTERS", "V2 Value & Variety", "Spirit", "RAMRAJ COTTON",
            "FRESH FEET", "Kuons Avenue", "BE AWARA", "Mystere Paris", "Mine4Nine", "VAN HEUSEN DENIM LABS",
            "The Roadster Lifestyle Co", "SEVEN by MS Dhoni", "VILLAIN", "GALYPSO", "Allen Cooper", "Crimsoune Club",
            "KHADIO", "PrettyPlus by Desinoor.com", "3PIN", "Snarky Gal", "Fame Forever by Lifestyle", "UNDER ARMOUR",
            "Aeropostale", "Porsorte", "Earthen BY INDYA", "Harry Potter By Sztori", "Get Wrapped", "Zotw",
            "BASIICS by La Intimo", "FRIENDS by DressBerry", "Yogue Activewear", "Samshek", "cape canary", "SG LEMAN",
            "UCLA", "PIRKO", "DeFacto", "Zink London", "MARC", "9teenAGAIN", "Chipbeys", "ATRAENTA",
            "BEAT LONDON by PEPE JEANS", "Netram", "Shangrila Creation", "ANI", "Rodzen", "Fabindia", "Badoliya & Sons",
            "JEWELS GEHNA", "Softskin", "Cosmo Lady", "IX IMPRESSION", "Women Touch", "ZNX Clothing", "MARC LOUIS",
            "THE SILHOUETTE STORE", "Ashnaina", "DYCA", "TRUNDZ", "KOMLI", "Kook N Keech Looney Tunes", "Crocodile",
            "Naari", "EVERDION", "Latin Quarters", "Forca", "Ed Hardy", "SG RAJASAHAB", "Reebok Classic",
            "American Bull", "SHUBHVASTRA", "FRENCH FLEXIOUS", "VAIRAGEE", "Horsefly", "U.S. Polo Assn. Tailored",
            "Peter England Elite", "Span", "fabGLOBAL", "ADA", "Beau Design", "Yaadleen", "Provogue", "Lotto",
            "American-Elm", "Sethukrishna", "PUMA Hoops", "Sztori Marvel", "Pothys", "Online Fayda", "HRITIKA",
            "SANGAM PRINTS", "Alsace Lorraine Paris", "VARUSHKA", "Metronaut", "AUSTIVO", "Southbay", "trueBrowns",
            "OOMPH", "MADAME M SECRET", "LYKKEIN", "AccessHer", "JoE Hazel", "Colors", "Vasudha", "Parx",
            "Tom & Jerry By Sztori", "LY2", "KRAFT INDIA", "THE MILLION CLUB", "Selvia", "AGIL ATHLETICA", "Be Indi",
            "Fruit of the loom", "HRX", "THE NKS PLUS", "Luni", "BharatSthali", "Soch", "evolove", "Reveira",
            "FabAlley Curve", "Roly Poly", "RIVI", "Newport", "Genx", "Almo Wear", "La Intimo", "HK colours of fashion",
            "Ecentric", "Madame", "Taanz", "U.S. Polo Assn. Women", "Zaveri Pearls", "Pashtush", "fashiol", "Bralux",
            "ACTIMAXX", "Adobe", "Llajja", "VEDANA", "pinwheel", "DesiNoor.com", "F.R.I.E.N.D.S By Sztori",
            "Forever Glam by Pantaloons", "BROOWL", "Wolfpack", "Van Heusen Flex", "VeBNoR", "KAAJH",
            "Om Shantam Sarees", "Ms.Lingies", "Not Just Pyjamas", "Modriba", "ARDEUR", "French Connection", "GANT",
            "Kastner", "AMOSIO", "Abhishti", "emeros", "EVADICT By Decathlon", "Chitwan Mohan", "KISAH PLUS", "Snitch",
            "Pierre Carlo", "Faserz", "UNITED LIBERTY", "Ramas", "Butt-Chique", "The Kaftan Company", "PINKSKY",
            "BRATMA", "Basics", "BLITZSOX", "Soul Space", "Marvel by Wear Your Mind", "HARBORNBAY", "Mufti",
            "Kook N Keech Harry Potter", "Juniper Plus", "Pistaa", "MUSCLE TORQUE", "BEYOUND SIZE - THE DRY STATE",
            "Jealous 21", "Spiritus by pantaloons", "Braveo", "IMT", "Nick&Jess", "SHERRY", "toothless",
            "Colour Me by Melange", "BUY NEW TREND", "ANTI CULTURE", "Lakshita", "FREAKINS", "Gallus", "KLOTTHE",
            "Heelium", "MOMIN LIBAS", "Invincible", "Ennoble", "Bishop Cotton", "Taurus", "SUTI", "Charu", "Seerat",
            "Stylee LIFESTYLE", "Kalyani", "MELON", "Trendyol", "Denimize by Fame Forever", "ONLY & SONS",
            "Louis Philippe ATHPLAY", "Blacksmith", "VASTRAMAY PLUS", "SOJANYA PLUS", "KOI SLEEPWEAR", "NoBarr",
            "Jade Garden", "AkaAyu", "Ms.Taken", "PRIDE APPAREL", "MASH UNLIMITED", "Raas", "armure", "Sztori DC",
            "Teakwood Leathers", "9shines Label", "Calvin Klein Innerwear", "Missguided", "CL SPORT", "MANGO MAN",
            "SF JEANS by Pantaloons", "Bareblow", "FOGA", "abof", "Peora", "THE WEAVE TRAVELLER", "SHANGRILA", "Ligalz",
            "MOKSHA DESIGNS", "Lenissa", "MISRI", "urSense", "DOROTHY PERKINS", "Denizen From Levis", "hangup plus",
            "Mint & Oak", "9rasa", "VAIVIDHYAM", "Clt.s", "PICOT", "Nakshi", "Svanik", "Lino Perros", "SPACES",
            "IMYOUNG", "AUSTIEX", "Being Fab", "Sztori Batman", "aayusika", "Kifahari", "TALES & STORIES", "ColorPlus",
            "Amirah s", "Sloggi", "ScoldMe", "ATTIITUDE", "MAXENCE", "Shree", "Smugglerz", "DC Comics",
            "XL LOVE by Janasya", "KARATCART", "Kiana", "ZUSH", "LUCERO", "BOWER", "YOLOCLAN", "ATHLISIS", "Solemio",
            "GIORDANO", "FITUP LIFE", "PERFLY By Decathlon", "SELECTED", "Aarika", "Tossido", "CHRISTEENA", "Fab Dadu",
            "NDS Niharikaa Designer Studio", "DIXCY SCOTT Slimz", "MAASHIE", "Sparx", "FOSH", "SuperBottoms", "PRENEA",
            "Kurti's by Menka", "FINSBURY LONDON", "FLAWLESS", "Noi", "Btwin By Decathlon", "Thomas Scott",
            "MASCLN SASSAFRAS", "Canary London", "TATTVA", "RANGOLI", "Indi INSIDE", "ZRI", "SUTRAM", "awesome",
            "SATYAM WEAVES", "Duchess", "Coucou", "Susie", "109F", "KAMI KUBI", "KHUMAAR Shuchi Bhutani",
            "Momsoon Maternity", "Aesthetic Bodies", "Ethnix by Raymond", "PAROKSH", "DEYANN PLUS", "Creeva",
            "Dream of Glory Inc", "MKH", "bedgasm", "MANOHARI", "BANARASI SILK WORKS", "GROVERSONS Paris Beauty",
            "Converse", "American Eye", "Ayaki", "CAVALLO by Linen Club", "Voi Jeans", "Varkha Fashion", "Wyfees",
            "shyaway", "Woodland", "PUNK", "Raa Jeans", "BUCIK", "Athleto", "V-Mart", "DEVOILER", "POKER ACTIVE",
            "SUBEA By Decathlon", "Star Wars by Wear Your Mind", "RANGMANCH PLUS by Pantaloons", "Youthnic",
            "Sanganeri Kurti", "YAVI", "Modern Indian by CHEMISTRY", "Priyaasi", "Skidlers", "Koton", "Juelle", "SG",
            "Solly Jeans Co.", "Kolor Fusion", "DECOREALM", "SockSoho", "FIFA U-17 WC", "rock.it", "Tusok", "SANISA",
            "INDYES", "SERONA FABRICS", "MAKCLAN", "PROTEENS", "Blush by PrettySecrets", "SmileyWorld",
            "Label Ritu Kumar", "ETIQUETTE", "aarke Ritu Kumar", "VINENZIA", "UrGear", "OBOW", "Luxrio", "NAKKASHI",
            "Rang Gali", "PNEHA", "I Jewels", "Anahi", "SHARAA ETHNICA", "Style SHOES", "PATRORNA",
            "Marigold by FableStreet", "RVK", "Kenneth Cole", "Masculino Latino", "THANGAMAGAN", "Big Fox",
            "American Crew Plus", "GLAM ROOTS", "MYAZA", "JAIPURI BUNAAI", "XOXO Design", "Lounge Dreams", "Modeve",
            "Fusion Beats", "Vaak", "ISU", "BEVERLY BLUES", "Kiaahvi by JOHN PRIDE", "IMPACKT", "Turtle", "Greenfibre",
            "SUITLTD", "Newfeel By Decathlon", "Jansons", "Iris", "curveZI by Ziyaa", "Ardozaa", "Anekaant", "nexus",
            "Lovely Lady", "Maishi", "Westwood", "NIXX", "Gipsy", "JC Mode", "bebe", "sandy AND ritz", "Columbia",
            "Bruun & Stengade", "Jean Cafe", "FILA", "Dais", "Forca by Lifestyle", "Mumbai Indians", "Manu",
            "WHITE HEART", "Sztori Superman", "STROP", "KAJARU", "Melangebox", "SURHI", "V SALES", "Silk Land",
            "Neerus", "LacyLook", "KICA", "MALHAAR", "Disney by Wear Your Mind", "Fabnest Curve", "OTORVA",
            "SILVER STOCK", "Woods", "SKULT by Shahid Kapoor", "Octave", "VEGA", "Black Panther", "Next Look",
            "Creature", "IVOC Plus", "MERLOT", "Fluffalump", "NBA", "DS WORLD", "Routeen", "EVOQ", "Alan Jones",
            "Saree Swarg", "PinkLoom", "Lagashi", "Charukriti", "Mesmore", "SAADHVI", "VENISA", "Do Dhaage", "GRIIHAM",
            "Laa Calcutta", "elleven", "Rapra The Label", "Berrys Intimatess", "ANTS", "wild U", "IMPERATIVE",
            "RAREISM", "YCANF", "DAiSY", "Hatheli", "Sayesha", "Toshee", "LA LOFT", "BODYACTIVE", "Simond By Decathlon",
            "SIMON CARTER LONDON", "Rex Straut Jeans", "KULTPRIT", "BAMBOS", "Alvaro Castagnino", "Dollar Socks",
            "Undercolors of Benetton", "Khoday Williams", "FirstKrush", "THREADCURRY", "VERO AMORE", "1 Stop Fashion",
            "AADVIKA", "ROOTED", "Havida Sarees", "Bavaria", "Kalakari India", "Atsevam", "Lavanya The Label",
            "Prag & Co", "Ikk Kudi by Seerat", "MeeMee", "Tuna London", "Head", "JAAIVE", "LEWAWAA", "ZOELLA", "shiloh",
            "Belliskey", "sindrellastorie", "Next One", "WTFUNK", "QUBIC", "JADE BLUE", "Street Armor by Pantaloons",
            "MUTAQINOTI", "New Balance", "FiTZ", "Obaan", "MANQ Plus", "SHOWOFF", "Linen Club", "MOHEY",
            "HOUSE OF KARI", "Glorious", "tantkatha", "Ritu Kumar", "Zelocity", "CIERGE", "SANSKRUTIHOMES",
            "DREAMSS BY SHILPA SHETTY", "Kosha", "Juliet", "Femmora", "Tiara", "DECKEDUP", "MARZENI", "BonOrganik",
            "ELLE", "iki chic", "VANCA ECO", "RUNWAYIN", "TARA-C-TARA", "CHIMPAAANZEE", "ROCKRIDER By Decathlon",
            "AD By Arvind", "Antony Morato", "INDIAN WOOTZ", "Modi Kurta", "AVI Living", "AESTHETIC NATION", "Soxytoes",
            "FERANOID", "Marvel Avengers", "Aventura Outfitters", "FINNOY", "SHRESTHA BY VASTRAMAY",
            "Wear Your Opinion", "Raano", "KIMAYRA", "Swasti", "SIRIL", "SINGNI", "PERFECT WEAR", "Aryavart",
            "Fusion Threads", "BLANC9", "House of JAMMIES", "Bruchi CLUB", "JerfSports", "Silvertraq",
            "LAMOURE BY RED CHIEF", "Fashion FRICKS", "klauressa", "NA-KD", "SAHORA", "Senyora", "Femme Luxe", "DELAN",
            "KBZ", "APSLEY", "MADHURAM", "COTTON ON", "GRASS by Gitika Goyal", "LINDBERGH", "HARSAM", "American Archer",
            "Vandnam Fabrics", "Waahiba", "Tasarika", "MOHANLAL SONS", "Anug by SOJANYA", "AGIL ARMERO",
            "Arrow Blue Jean Co.", "EPPE", "SHIRT THEORY", "MILLENNIAL MEN", "SHOWOFF Plus", "WHITE FIRE", "Wodreams",
            "Adwitiya Collection", "Awadhi", "Jinfo", "Silvermerc Designs", "Ziva Fashion", "Meena Bazaar",
            "PARAMOUNT CHIKAN", "Safaa", "araaliya", "The Mini NEEDLE", "Minora", "BEATITUDE", "FLAVIDO", "akheri",
            "GILLORI", "Bene Kleed Plus", "shopbloom", "Planetinner", "Theater", "JUNAROSE", "VAABASTA", "DESI BEATS",
            "Sibi", "HELLO DESIGN", "THE CLOTHING FACTORY", "Fashion Cult", "LABEL REGALIA", "Insua", "ZALORA BASICS",
            "ZALORA OCCASION", "Diwaah", "Wabii", "Miss Bennett", "SDL by Sweet Dreams", "Oxemberg", "Mark Leute",
            "LOUIS STITCH", "Luxure by Louis Philippe", "Royal Enfield", "mode de base", "ACTOHOLIC", "Swiss Club",
            "JDC", "Beverly Hills Polo Club", "Franco Leone", "Status Quo", "Armaan Ethnic", "rasm",
            "Uri and MacKenzie", "Dennis Morton", "Texlon", "WITH", "iO", "VENITIAN", "883 Police", "YOONOY", "Pepe",
            "berry blues", "PockMAN", "Thara Sarees", "Nirkhi", "CATCHY", "ORTANGE", "Saraf RS Jewellery",
            "Indiankala4u", "Kalamandir", "Maybell", "Charak", "ANWAIND", "COTCLO", "MYLO ESSENTIALS", "Bwitch",
            "College Girl", "Truerevo", "Campus", "La Aimee", "SHECZZAR", "Saaki", "Hapuka", "Splash",
            "Sitaram Designer", "PAPA BRANDS", "CUSHYBEE", "SHFFL", "Ashlee", "am ma", "Altiven", "FLAMBOYANT",
            "Owncraft", "J Turritopsis", "ROZVEH", "Deal Jeans", "METTLE", "BEN SHERMAN", "Balista", "SELVAMAGAN",
            "LUXURAZI", "DON VINO", "bummer", "Copperline", "Red Chief", "Burnt Umber", "FREESOUL", "POONAM DESIGNER",
            "Indethnic", "Sudarshan Silks", "SHINGORA", "Mafadeny", "CLOSETHOOK", "TRISHAA BY PANTALOONS", "aLL",
            "True Blue", "Masch Sports", "SPARROWHAWK", "East Ink", "pivot", "Matinique", "Resha", "Arhi", "PACHE",
            "Unnati Silks", "The Chennai Silks", "Bani Women", "anokherang", "DEALSEVEN FASHION",
            "WEST VOGUE by Zivame", "FITLEASURE", "Fleximaa", "COUPER & COLL", "LastInch", "Fleurt", "ZALORA WORK",
            "CHARMGAL", "NUSH", "VS", "Lawman pg3", "wHAT'S DOwn", "Canterbury", "Parcel Yard", "YOVISH",
            "PLATINUM Studio", "Nimayaa", "gvs shoppe", "navyasa", "Ratan Creation", "Somras", "FAWOMENT",
            "Pyjama Party", "Wear Equal", "Apratim", "berrytree", "DEGE", "RAASSIO", "Quinoa", "Istyle Can", "aaliya",
            "MINGLAY", "Ojas Designs", "MAIYEE", "Kuber Studio", "Kira", "OKHAI", "NIZA", "Twenty3", "CAMLA",
            "J Hampstead", "TNG", "BODYX", "Speedo", "Donzell", "La Bele", "Softline Butterfly", "next", "Donald Duck",
            "CHROME & CORAL", "GUESS", "JUNK de LUXE", "s.Oliver", "GAS", "SPORTO", "Macroman M-Series", "Superdry",
            "OVS", "ESPRIT", "ORIGIN BY ZALORA", "AD & AV", "WAIMEA", "Numero Uno", "IZOD", "ZALORA ACTIVE",
            "American Swan", "Killer", "KRA", "FURO by Red Chief", "Wills Lifestyle", "SELA", "EDRIO",
            "Van Heusen ACADEMY", "Old Grey", "Slub", "Fox", "Tiktauli De.Corps.", "Blue Saint", "Disrupt",
            "Solly Sport", "Nature Casuals", "VEI SASTRE", "Emerals", "Double Two", "I Am Animal", "Pure Play",
            "ELABORADO", "39 THREADS", "FITINC", "Reich Color", "Santonio", "MERCHANT MARINE", "RUG WOODS", "EVERBLUE",
            "Srey trends", "METAL", "Sam and Jack", "La Mode", "JB STUDIO", "Callino London", "MR BUTTON", "Mr. Button",
            "CANOE", "TAHVO", "PEPLOS", "Masculino Latino Plus", "VUDU", "ULTRAMATE", "COLVYNHARRIS JEANS", "Theme",
            "AVOLT", "Masaba", "Manish Creations", "amzira", "British Club", "Lombard", "HOB", "Kaifoo", "azania",
            "MANAMAGAN", "PIVOTO", "N Dot", "MEWAR", "VARUDU", "RUDRAKSH", "PEONY SMART WORLD",
            "Pro-Ethic STYLE DEVELOPER", "Peony Cotton Fab", "Aj DEZInES", "Enciger", "OFFIRA TEX WORLD", "ethnix",
            "SHVAAS by VASTRAMAY", "PRAKASAM COTTON", "INDIAN EPIC", "KAVYA SAREES", "GoStyle", "VARA SILK",
            "Baawara By Bhama", "Glemora", "LAGAAV", "MODI JACKET", "ORUS", "Shahjada", "SI2 SLIP IN 2",
            "Indian Poshakh", "Bunnywave", "SIDEWOK", "The Tie Hub", "EL REGALO", "BuckleUp", "CRUSSET", "Bharatasya",
            "RAD.MAD.BAD", "SMUGGLERZ INC.", "SWHF", "hexafun", "Aura", "POPLINS", "CASA DE NEE NEE", "CENWELL",
            "Mojama", "LUX MAESTRO", "Aptonia By Decathlon", "Home Centre", "Chromozome", "Happy Socks",
            "Geonaute By Decathlon", "LAVOS", "SayItLoud", "OUTSHOCK By Decathlon", "Arsenal FC", "Admiral", "Champion",
            "Li-Ning", "Russell Athletic", "ASICS Tiger", "BRACLO", "PRINCINN MEYER", "MotoGP", "Ted Smith",
            "FFLIRTYGO", "Zarlin", "William White", "Kawach", "5TH ANFOLD", "GullyActive", "Juventus", "SORATIA",
            "Cloak & Decker by Monte Carlo", "KINGDOM OF WHITE", "Pootlu", "Huggun", "effy", "WELBAWT", "D Kumar",
            "MR.KAMEEJ", "WILD WEST Plus", "Zeal", "WYRE", "Firangi Yarn", "THE BONTE", "JACKDANZA", "JAPS",
            "Metersbonwe", "Arctic Fox", "CROYDON", "Jashvi Creation", "JJAAGG T", "ahhaaaa", "Cloak & Decker",
            "Blazer Quarter", "Fully Filmy", "Pactorn", "DKGF FASHION", "Success", "Four One Oh", "FC Barcelona",
            "Rick Masch", "BLACK RADIO", "Jolie robe", "HOLDIT", "PEPPYZONE", "SIAPA", "Tiktauli De Corps.",
            "Latest Chikan Garments", "Mode Vetements", "Jansi", "Fibre World", "DRESSTIVE", "Warthy Ent", "ADITRI",
            "Moda Chales", "Taneira", "BOMBAY SELECTIONS", "Bangalore Silk Emporium", "KARAGIRI", "Hastakala",
            "MONJOLIKA FASHION", "Banarasi Style", "Efulgenz", "Monk & Mei", "APCO", "Fabclub", "JAVTA", "DIVINATION",
            "LAHEJA", "Kushal's Fashion Jewellery", "Vogue Era", "Skirans", "House of Dhaaga", "Mizash", "Marcia",
            "thickskin", "itse", "MS RETAIL", "Lilots", "THE AAB STUDIO", "ALC Creations", "Yashika",
            "KLM Fashion Mall", "Fashionuma", "shashvi", "mirari", "Indian Dobby", "Castle Lifestyle", "AMUKTI",
            "KOOCHI POOCHI", "Aarsha", "La Vastraa", "Rivana", "PERFECTBLUE", "Pankvi", "Jaipur Morni", "MIRAVAN",
            "Shopping Queen", "PK Fashions", "One of a Kind", "KEEP CART", "Alaya By Stage3", "Spera", "SKY SHOPPIE",
            "Feather Soft Elite", "Anaita", "RangDeep", "Women Republic", "Nahara", "RGHT", "ArtEastri", "UNTUNG",
            "taruni", "AMIRAS INDIAN ETHNIC WEAR", "SVARCHI", "I Saw It First", "Royal Rajgharana Saree",
            "SWADHA FASHIONS", "DUGRI BE THE ONE", "PINKVILLE JAIPUR", "HAVVA FASHION", "NITVAN", "Scarves & Glitters",
            "AKISO", "arangya", "KETAKI FASHION", "Fashion Booms", "Mizzific", "EZIS FASHION", "Swtantra", "SAABHI",
            "kriatma", "ANIKAS CREATION", "TRISHLA INDIA", "SABOO COLLECTIONS", "The Kurta Express", "MAFE", "Mirraw",
            "Aanyor", "Peppertree", "IQRAAR", "Cot'N Soft", "ArtiZenWeaves", "Ekta Textiles", "OFFICE & YOU",
            "Lokatita", "DART STUDIO", "Ancestry", "ERISHA", "Fashion Basket", "Dipsha", "Vrundavan ethics", "Wuxi",
            "IRIDAA JAIPUR", "NABIA", "PARIKA CREATION", "Touch Trends", "Miss Poem", "Moedbuille", "Cloth Haus India",
            "KAJREE", "CLAI WORLD", "GUNVANTI FAB", "Leeza Store", "DURVI", "OMASK", "Chokhi Bandhani", "ADEESHA",
            "CKM", "Fentacia", "PMD Fashion", "LABEL AARNA", "Biswa Bangla", "BoStreet", "Orbbaan", "ESME", "Saksh",
            "DORIYA", "Rosso Milano", "SBR BABA KURTI", "maaisarah", "CHOWDHRAIN", "Ambraee", "Chidiyaa", "LELA",
            "MODARTA", "Pratham Blue", "Rudra Fashion", "SARIYA", "Lovista", "ASPORA", "Love More", "MIKHAD", "KUPINDA",
            "FIROZA", "lal10", "FINE WEAR", "IkDaiya", "Kurtipedia", "MAAND", "Exclusiva", "CHICQUITA", "Pandadi Saree",
            "kasee", "Lavanya", "YELLOW PARROT", "Kesarya", "Ayesha", "Wicked Stitch", "soan", "justpeachy",
            "Binnis Wardrobe", "PS PRET BY PAYAL SINGHAL", "Zelen", "HOUSE OF JAMOTI", "NAINA ARUNIMA", "Shuddhi",
            "REME", "FEMMIBELLA", "JSItaliya", "MUFFLY", "INDDUS PLUS", "Zanani INDIA", "Saart Bunaai", "FASHION DWAR",
            "Linen Club Woman", "Sajke", "PHEETA", "Threeness", "Thread & Button", "Ansar Creation", "SHRINKHLA",
            "MEERA FAB", "Mulmul By Arabella", "HANDME", "Rangpur", "NeshamaKurti", "neckbook", "SAPTRANGI",
            "CHARISMOMIC", "blue hour", "Dazzle", "MAYSIXTY", "CURWISH", "VASTRADO", "July Nightwear", "POKORY",
            "Penny", "SPIRIT ANIMAL", "YOU FOREVER", "PARKHA", "YAMAMAY", "Private Lives", "STUDIOACTIV", "NOCHEE VIDA",
            "SARINA", "Putchi", "La Senza", "POORAK", "ROVARS", "OXXO", "BStories", "Bell Paper", "BOLDFIT", "SATVA",
            "MIDNIGHT ANGELS BY PC", "Vvoguish", "UNMADE", "Katn India", "VERO MODA EASE", "Herryqeal",
            "Lavika Couture", "Looney Tunes by Dressberry", "Ever Loved", "Sidwa", "urban undress", "Hunkemoller",
            "HILL STREET", "Cartoon Network by Dressberry", "Proyog", "CRAFIQA", "R26", "Catnap", "VELOZ", "HAPSELL",
            "Zalza", "Chicco", "Macrowoman W-Series", "Anaario", "AVANT-GARDE PARIS", "VStar", "Tailor & Circus",
            "DKNY", "LAASA  SPORTS", "ALTOMODA by Pantaloons", "Restless", "LAYA", "Active Soul", "London Rag", "SEEQ",
            "Mizuno", "KENDALL & KYLIE", "BARARA ETHNIC", "SEW YOU SOON", "JUNE & HARRY", "Denap", "AMMARZO",
            "The Label Life", "OHA BOY", "UnaOne", "OPt", "KLAS NOBL", "MERCHE", "Popwings", "GRACE & JACK", "IDK",
            "Crozo By Cantabil", "250 DESIGNS", "Her Grace", "MODLI 20 FASHION", "angloindu", "Emprall", "Miramor",
            "Kannan", "Mesmerize", "Species", "Ekmatra", "Riot Jeans", "Mom For Sure by Ketki Dalal", "XO LOVE",
            "Moms Maternity", "bellamia", "echt", "BEAVER", "SHEETAL Associates", "GRECIILOOKS", "FABISTA",
            "Label Tamanna Rungta", "Charitra", "Cottonworld", "Rizzly", "A.T.U.N.", "CINNAMON CLOSET", "POPPI",
            "DAEVISH", "VERO MODA CURVE", "Trufit", "Arrabi", "ewoke", "Reistor", "PReT me", "RHHENSO", "Scotch & Soda",
            "House of Mool", "ATTIC SALT", "Paralians", "Young Trendz", "Ourdve", "CULT FICTION", "ALCOTT", "ANAN",
            "WOOFFLY", "Boohoo", "zink Z", "Fab Star", "Anysa", "Qsymia", "THE UKKIYO LIFE", "IndusDiva Loomnic",
            "Kiyashi", "STYL CO.", "Rattrap", "Oumbre", "DUDITI", "Sisley", "promod", "ELEVEN.O.ONE", "Remanika",
            "EVAH LONDON", "IndusDiva Infusions", "Arrow Woman", "Guniaa", "Little Musketeer", "Encrypt", "FCK-3",
            "GLAMAZE", "RED CHERI", "radka", "the kaatn trail", "pink woman", "AUDSTRO", "Doodlage", "CHOZI",
    "STATUS MANTRA" };
    public String[] clothingType = { "Men jeans", "Men track-pants", "Men shirts", "Women shapewear", "Women tshirts",
            "Women tops", "Men trousers", "Men tights", "Men tshirts", "Women kurta-sets", "Women jumpsuit",
            "Women kurtas", "Women trousers", "Women bra", "Women shirts", "Women shorts", "Women dresses",
            "Women bath-robe", "Women track-pants", "Women tights", "Women jackets", "Men socks", "Women jeans",
            "Men briefs", "Women briefs", "Women sweatshirts", "Women sarees", "Men trunk", "Women kurtis",
            "Women skirts", "Women night-suits", "Men jackets", "Men sweatshirts", "Men lounge-pants", "Women palazzos",
            "Women stockings", "Women jeggings", "Women leggings", "Women lounge-pants", "Women shrug", "Men kurtas",
            "Men boxers", "Men shorts", "Women dupatta", "Men kurta-sets", "Women tunics", "Men innerwear-vests",
            "Men sweaters", "Women sweaters", "Men lounge-shorts", "Women thermal-tops", "Women capris",
            "Women nightdress", "Men pyjamas", "Women sports-sandals", "Women dungarees", "Men tracksuits",
            "Women camisoles", "Men nehru-jackets", "Women blazers", "Women thermal-bottoms", "Men lounge-tshirts",
            "Women lounge-shorts", "Women lehenga-choli", "Women baby-dolls", "Women coats", "Women pyjamas",
            "Men thermal-set", "Men thermal-bottoms", "Men blazers", "Women saree-blouse", "Men churidar",
            "Women tracksuits", "Women dress-material", "Women boots", "Men thermal-tops", "Women lingerie-set",
            "Men sherwani", "Women co-ords", "Women flats", "Women swimwear", "Women rain-jacket", "Women socks",
            "Women patiala", "Women salwar", "Women harem-pants", "Men bath-robe", "Women patiala-and-dupatta",
            "Women lingerie-accessories", "Women saree-accessories", "Men suits", "Men dhotis", "Women shawl",
            "Men rain-jacket", "Men salwar", "Men swim-bottoms", "Women outdoor-masks", "Women stoles",
            "Women clothing-set", "Men night-suits", "Men coats", "Women dhotis", "Men shawl", "Women churidar",
            "Women robe", "Women earrings", "Women casual-shoes", "Women salwar-and-dupatta", "Men swimwear",
            "Women scarves", "Women slips", "Men waistcoat", "Men leggings", "Women burqas", "Women lounge-tshirts",
            "Men patiala", "Women waistcoat", "Women necklace-and-chains", "Women nehru-jackets",
            "Women hair-accessory", "Women sleepsuit", "Men dupatta", "Men co-ords", "Men clothing-set", "Women heels",
            "Men lungi", "Women bracelet", "Women jewellery-set", "Women handbags", "Women flip-flops",
            "Women Indian & Fusion Wear", "Women Kurtas & Suits", "Women Kurtis, Tunics & Tops", "Women Sarees",
            "Women Ethnic Wear", "Women Leggings, Salwars & Churidars", "Women Skirts & Palazzos",
            "Women Dress Materials", "Women Lehenga Cholis", "Women Dupattas & Shawls", "Women Jackets",
            "Women Belts, Scarves & More", "Women Watches & Wearables", "Women Western Wear", "Women Dresses",
            "Women Tops", "Women Tshirts", "Women Jeans", "Women Trousers & Capris", "Women Shorts & Skirts",
            "Women Co-ords", "Women Playsuits", "Women Jumpsuits", "Women Shrugs", "Women Sweaters & Sweatshirts",
            "Women Jackets & Coats", "Women Blazers & Waistcoats", "Women Plus Size", "Women Maternity",
            "Women Sunglasses & Frames", "Women Flats", "Women Casual Shoes", "Women Heels", "Women Boots",
            "Women Sports Shoes & Floaters", "Women Sports & Active Wear", "Women Clothing", "Women Footwear",
            "Women Sports Accessories", "Women Sports Equipment", "Women Lingerie & Sleepwear", "Women Bra",
            "Women Briefs", "Women Shapewear", "Women Sleepwear & Loungewear", "Women Swimwear",
            "Women Camisoles & Thermals", "Women Beauty & Personal Care", "Women Makeup", "Women Skincare",
            "Women Premium Beauty", "Women Lipsticks", "Women Fragrances", "Women Gadgets", "Women Smart Wearables",
            "Women Fitness Gadgets", "Women Headphones", "Women Speakers", "Women Jewellery", "Women Fashion Jewellery",
            "Women Fine Jewellery", "Women Earrings", "Men Topwear", "Men T-Shirts", "Men Casual Shirts",
            "Men Formal Shirts", "Men Sweatshirts", "Men Sweaters", "Men Jackets", "Men Blazers & Coats", "Men Suits",
            "Men Rain Jackets", "Men Indian & Festive Wear", "Men Kurtas & Kurta Sets", "Men Sherwanis",
            "Men Nehru Jackets", "Men Dhotis", "Men Bottomwear", "Men Jeans", "Men Casual Trousers",
            "Men Formal Trousers", "Men Shorts", "Men Track Pants & Joggers", "Men Innerwear & Sleepwear",
            "Men Briefs & Trunks", "Men Boxers", "Men Vests", "Men Sleepwear & Loungewear", "Men Thermals" };

    public String[] fabricType = { "Cotton", "Elastane", "Linen", "Modal", "Nylon", "Polyester", "Satin", "Silk",
            "Viscose Rayon", "Acrylic", "Corduroy", "Cotton Canvas", "Cotton Linen", "Crepe", "Cupro", "Denim",
            "Elastane", "Georgette", "Hemp", "Khadi", "Leather", "Linen Blend", "Lyocell", "Modal", "Organic Cotton",
            "Poly Silk", "Polycotton", "Silk", "Smart Fit", "Suede", "Synthetic", "" };

    public String[] Occasion = { "Casual", "Formal", "Party", "Semiformal", "" };

    public String[] menAccessories = { "Men Plus Size", "Men Footwear", "Men Casual Shoes", "Men Sports Shoes",
            "Men Formal Shoes", "Men Sneakers", "Men Sandals & Floaters", "Men Flip Flops", "Men Socks",
            "Men Personal Care & Grooming", "Men Sunglasses & Frames", "Men Watches", "Men Sports & Active Wear",
            "Men Sports Shoes", "Men Sports Sandals", "Men Active T-Shirts", "Men Track Pants & Shorts",
            "Men Tracksuits", "Men Jackets & Sweatshirts", "Men Sports Accessories", "Men Swimwear", "Men Gadgets",
            "Men Smart Wearables", "Men Fitness Gadgets", "Men Headphones", "Men Speakers", "Men Fashion Accessories",
            "Men Wallets", "Men Belts", "Men Perfumes & Body Mists", "Men Trimmers", "Men Deodorants",
            "Men Ties, Cufflinks & Pocket Squares", "Men Accessory Gift Sets", "Men Caps & Hats",
            "Men Mufflers, Scarves & Gloves", "Men Phone Cases", "Men Rings & Wristwear", "Men Helmets" };

    private Random random;

    public Predictor<String, float[]> predictor = null;
    static final String digits = "0123456789";
    static final char[] key_chars = (digits).toCharArray();
    String randomString;
    public static int flt_buf_length;
    boolean mockVector = false;
    public static float[] flt_buf;
    private WorkLoadSettings ws;

    public Vector(WorkLoadSettings ws) {
        super();
        this.ws = ws;
        this.random = new Random();
        if(ws != null && !ws.mockVector) {
            this.setEmbeddingsModel(ws.model);
            return;
        }
        Vector.flt_buf = new float[1024*1024];

        for (int index=0; index<1024*1024; index++) {
            Vector.flt_buf[index] = this.random.nextFloat();

        }
        Vector.flt_buf_length = Vector.flt_buf.length;
    }

    public Vector() {
        super();
    }

    public void setEmbeddingsModel(String DJL_MODEL) {
        String DJL_PATH = "djl://ai.djl.huggingface.pytorch/" + DJL_MODEL;
        Criteria<String, float[]> criteria =
                Criteria.builder()
                .setTypes(String.class, float[].class)
                .optModelUrls(DJL_PATH)
                .optEngine("PyTorch")
                .optTranslatorFactory(new TextEmbeddingTranslatorFactory())
                .optProgress(new ProgressBar())
                .build();
        ZooModel<String, float[]> model = null;
        try {
            model = criteria.loadModel();
        } catch (ModelNotFoundException | MalformedModelException | IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.predictor = model.newPredictor();
    }

    public static byte[] floatsToBytes(float[] floats) {
        byte bytes[] = new byte[Float.BYTES * floats.length];
        ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(floats);

        return bytes;
    }

    public static String convertToBase64Bytes(float[] floats) {
        return Base64.getEncoder().encodeToString(floatsToBytes(floats));
      }

    private float[] get_float_array(int length, Random random_obj) {
        int _slice = random_obj.nextInt(this.flt_buf_length - length);
        return Arrays.copyOfRange(this.flt_buf, _slice, _slice+length);
    }

    public Object next(String key) {
        this.random.setSeed(key.hashCode());
        float[] vector = null;
        String productDescription = "";
        productDescription += this.colors[this.random.nextInt(this.colors.length)] + " color ";
        productDescription += this.fabricType[this.random.nextInt(this.fabricType.length)] + " fabric ";
        productDescription += this.Occasion[this.random.nextInt(this.Occasion.length)] + " wear ";
        productDescription += this.clothingType[this.random.nextInt(this.clothingType.length)];
        productDescription += " by " + this.fashionBrands[this.random.nextInt(this.fashionBrands.length)];

        if(this.ws.mockVector) {
            vector = this.get_float_array(this.ws.dim, this.random);
        } else {
            try {
                vector = this.predictor.predict(productDescription);
            } catch (TranslateException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        if(!this.ws.base64)
            return new Product1(key, productDescription, vector);
        return new Product2(key, productDescription, convertToBase64Bytes(vector));
    }

    public class Product1 {

        @JsonProperty
        private String productID;
        @JsonProperty
        private float[] embedding;
        @JsonProperty
        private String productDescription;

        @JsonCreator
        public
        Product1(
                @JsonProperty("productID") String productID,
                @JsonProperty("productDescription") String productDescription,
                @JsonProperty("embedding") float[] vector){
            this.productID = productID;
            this.productDescription = productDescription;
            this.embedding = vector;
        }

        public String getProductID() {
            return this.productID;
        }

        public void setProductID(String productID) {
            this.productID = productID;
        }

        public String getProductDescription() {
            return this.productDescription;
        }

        public void setProductDescription(String productDescription) {
            this.productDescription = productDescription;
        }

        public float[] getEmbedding() {
            return this.embedding;
        }

        public void setEmbedding(float[] embedding) {
            this.embedding = embedding;
        }
    }

    public class Product2 {

        @JsonProperty
        private String productID;
        @JsonProperty
        private String embedding;
        @JsonProperty
        private String productDescription;

        @JsonCreator
        public
        Product2(
                @JsonProperty("productID") String productID,
                @JsonProperty("productDescription") String productDescription,
                @JsonProperty("embedding") String vector){
            this.productID = productID;
            this.productDescription = productDescription;
            this.embedding = vector;
        }

        public String getProductID() {
            return this.productID;
        }

        public void setProductID(String productID) {
            this.productID = productID;
        }

        public String getProductDescription() {
            return this.productDescription;
        }

        public void setProductDescription(String productDescription) {
            this.productDescription = productDescription;
        }

        public String getEmbedding() {
            return this.embedding;
        }

        public void setEmbedding(String embedding) {
            this.embedding = embedding;
        }
    }
}
