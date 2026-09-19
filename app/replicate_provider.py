"""Replicate adapters for Pruna image and video models."""
import os
import hashlib
import time
from pathlib import Path
from .config import ROOT, STYLE
from . import store

IMAGE_MODEL='google/nano-banana'
IMAGE_EDIT_MODEL='google/nano-banana'
VIDEO_MODEL='prunaai/p-video-2'

def media_models():
    """The fixed production media stack, recorded in each job manifest."""
    return {'image':IMAGE_MODEL,'image_edit':IMAGE_EDIT_MODEL,'video':VIDEO_MODEL}

def _safe_error(exc):
    detail=str(exc)
    token=os.getenv('REPLICATE_API_TOKEN','')
    if token:detail=detail.replace(token,'[redacted]')
    return detail[:600]

class Replicate:
    def __init__(self, token, checkpoint):
        if not token.strip():
            raise RuntimeError('REPLICATE_API_TOKEN is missing. No generation was started.')
        self.checkpoint=checkpoint
        import replicate
        self.client=replicate.Client(api_token=token)

    def _predict(self, model, inp, path, records):
        """Journal before submitting; an ambiguous submission is never submitted again."""
        journal=path.with_suffix('.operation.json')
        saved=store.read(journal) if journal.exists() else None
        signature=hashlib.sha256(str((model,records)).encode()).hexdigest()
        if saved and saved.get('signature')!=signature:
            raise RuntimeError('Saved prediction inputs differ. Inspect the existing operation before changing it.')
        if saved and not saved.get('id'):
            raise RuntimeError('Prediction submission outcome is uncertain. Inspect Replicate history and reconcile the saved operation before retrying.')
        if saved:
            prediction=self.client.predictions.get(saved['id'])
        else:
            version=self.client.models.get(model).latest_version.id
            saved={'model':model,'inputs':records,'signature':signature,'status':'submitting','done':False}
            store.save(journal,saved)
            prediction=self.client.predictions.create(version=version,input=inp)
            saved['id']=prediction.id
            store.save(journal,saved)
        deadline=time.monotonic()+1800
        while True:
            saved.update(status=prediction.status,output=prediction.output,done=prediction.status in ('succeeded','failed','canceled'))
            store.save(journal,saved)
            if saved['done']:break
            self.checkpoint()
            if time.monotonic()>deadline:raise RuntimeError('Prediction still running; resume the saved operation.')
            time.sleep(2)
            prediction=self.client.predictions.get(saved['id'])
        if prediction.status!='succeeded':
            detail=str(getattr(prediction,'error',None) or 'No provider error was returned.')[:500]
            raise RuntimeError(f'Replicate prediction {prediction.status}: {detail}')
        output=prediction.output
        if isinstance(output,list):output=output[0]
        if hasattr(output,'read'):return output.read()
        import httpx
        with httpx.Client(follow_redirects=True,timeout=120) as client:
            response=client.get(str(output));response.raise_for_status()
            return response.content

    def structured(self,prompt,schema):
        from . import openai_text
        return openai_text.structured(prompt,schema,self.checkpoint)

    def image(self,prompt,path,refs=(),aspect='16:9'):
        if aspect!='16:9':raise ValueError('Pruna production requires 16:9.')
        self.checkpoint()
        visual_reference=ROOT/'app/visual-tone-reference.png'
        all_refs=([('STYLE-REFERENCE',visual_reference)] if visual_reference.exists() else [])+list(refs)
        model=IMAGE_EDIT_MODEL if all_refs else IMAGE_MODEL
        styled_prompt=(prompt if prompt.startswith(STYLE) else STYLE+'\n'+prompt)
        if visual_reference.exists():
            styled_prompt+='\nREFERENCE IMAGE ORDER: Image 1 is the visual-tone reference only: match its mature hand-drawn 2D illustration, restrained proportions, dark ink linework, teal rainy lighting and cinematic composition. Do not copy its man, bus, pose or layout. Images 2 onward are canonical scene references; preserve their identities, outfits and objects exactly.'
        inp={'prompt':styled_prompt,'aspect_ratio':aspect,'output_format':'png'}
        records={**inp,'references':[{'tag':tag,'file':ref.name,'sha256':hashlib.sha256(ref.read_bytes()).hexdigest()} for tag,ref in all_refs]}
        handles=[]
        if all_refs:
            inp['image_input']=[]
            for _,ref in all_refs:
                h=open(ref,'rb'); handles.append(h); inp['image_input'].append(h)
        try:
            data=self._predict(model,inp,path,records)
        except Exception as exc:
            raise RuntimeError(f'Replicate image generation failed: {_safe_error(exc)}') from None
        finally:
            for h in handles:h.close()
        from io import BytesIO
        from PIL import Image, ImageOps
        with Image.open(BytesIO(data)) as generated:
            converted=ImageOps.fit(generated.convert('RGB'),(1024,576),method=Image.Resampling.LANCZOS,centering=(0.5,0.5))
            buffer=BytesIO();converted.save(buffer,format='PNG');data=buffer.getvalue()
        temp=path.with_suffix('.tmp');temp.write_bytes(data);temp.replace(path)

    def video(self,prompt,image,path,aspect='16:9'):
        if aspect!='16:9':raise ValueError('Pruna production requires 16:9.')
        self.checkpoint()
        try:
            with open(image,'rb') as handle:
                inp={'prompt':prompt,'image':handle,'aspect_ratio':aspect,'duration':8,'draft':False,'prompt_upsampling':False}
                records={**inp,'image':{'file':image.name,'sha256':hashlib.sha256(image.read_bytes()).hexdigest()}}
                data=self._predict(VIDEO_MODEL,inp,path,records)
        except Exception as exc:
            raise RuntimeError(f'Replicate video generation failed: {_safe_error(exc)}') from None
        temp=path.with_suffix('.partial.mp4');temp.write_bytes(data);temp.replace(path)
