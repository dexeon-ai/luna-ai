import zipfile
from PIL import Image, ImageDraw, ImageFont

def make_image(filename, color, text):
    img = Image.new("RGBA", (400, 400), color=color)
    d = ImageDraw.Draw(img)
    try:
        font = ImageFont.truetype("arial.ttf", 40)
    except:
        font = ImageFont.load_default()
    # Use textbbox instead of deprecated textsize
    bbox = d.textbbox((0, 0), text, font=font)
    w, h = bbox[2] - bbox[0], bbox[3] - bbox[1]
    d.text(((400 - w) / 2, (400 - h) / 2), text, font=font, fill=(255, 255, 255))
    img.save(filename)

# Create starter images
make_image("avatar_base.png", "purple", "LUNA BASE")
make_image("mouth_rest.png", "black", "MOUTH REST")
make_image("mouth_mid.png", "darkred", "MOUTH MID")
make_image("mouth_open.png", "red", "MOUTH OPEN")

# Zip them
with zipfile.ZipFile("luna_assets.zip", "w") as zipf:
    for fn in ["avatar_base.png", "mouth_rest.png", "mouth_mid.png", "mouth_open.png"]:
        zipf.write(fn)

print("✅ Created luna_assets.zip with starter PNG assets.")
